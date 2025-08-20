from datetime import datetime, timedelta
from typing import Dict, Any
from google.analytics.data_v1beta import BetaAnalyticsDataClient # type: ignore
from google.analytics.data_v1beta.types import ( # type: ignore
DateRange,
Dimension,
Metric,
RunReportRequest,
Filter,
FilterExpression,
FilterExpressionList,
)
from flask import jsonify, request, Response
import functions_framework # type: ignore
import json
import os
import time
import traceback
from google.oauth2 import service_account # type: ignore
import tempfile
import logging
from urllib.parse import urlparse, parse_qs
from google.cloud import aiplatform
from vertexai.generative_models import GenerativeModel # type: ignore
import tiktoken  # type: ignore # トークン数計算用ライブラリを追加
import re
from concurrent.futures import ThreadPoolExecutor, as_completed  # 並列処理用
import threading
import uuid  # リクエスト識別子用
import random  # レート制限対応用
from functools import wraps  # デコレータ用
from ga4_summary import fetch_summary_data  # 事前集計機能をインポート


# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# グローバル変数の初期化

gemini_model = None
_model_lock = threading.Lock()
PROJECT_ID = os.environ.get("PROJECT_ID", "default-project-id")
LOCATION = os.environ.get("LOCATION", "us-central1")

def create_request_id():
    """リクエスト識別子を生成"""
    return str(uuid.uuid4())[:8]


def setup_request_logger(request_id):
    """リクエスト専用のロガーを設定"""
    logger = logging.getLogger(f"ga4_analysis_{request_id}")
    formatter = logging.Formatter(f'[{request_id}] %(asctime)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def init_vertexai():
    """VertexAIのGeminiモデルを初期化する（スレッドセーフ版）"""    
    global gemini_model

    try:
        if gemini_model is not None:
            return
        
        with _model_lock:
            # ダブルチェックロッキングパターン
            if gemini_model is not None:
                return
            
            logger.info("VertexAI初期化を開始...")
            from google.cloud import aiplatform
            from vertexai.generative_models import GenerativeModel # type: ignore
            
            aiplatform.init(project=PROJECT_ID, location=LOCATION)
            gemini_model = GenerativeModel("gemini-2.5-pro")
            logger.info("VertexAI初期化完了")
    except Exception as e:
        logger.error(f"VertexAI初期化エラー: {str(e)}")
        traceback.print_exc()
        raise e

def generate_ai_response(prompt, max_retries=3, delay_seconds=2):
    """Geminiモデルを使ってAI応答を生成する（リトライ機能付き）"""
    global gemini_model

    tries = 0
    last_error = None

    while tries < max_retries:
        try:
            # VertexAIが初期化されていない場合は初期化
            if gemini_model is None:
                init_vertexai()
            
            # プロンプトでAI応答を生成
            logger.info(f"AI応答生成開始（試行回数: {tries + 1}）")
            start_time = time.time()
            response = gemini_model.generate_content(prompt)
            elapsed_time = time.time() - start_time
            logger.info(f"AI応答生成完了 ({elapsed_time:.2f}秒)")
            
            # 応答テキストを返す
            response_text = response.text
            logger.info(f"AI応答生成: {len(response_text)} 文字")
            return response_text, elapsed_time
        except Exception as e:
            tries += 1
            last_error = e
            logger.warning(f"AI応答生成エラー（試行回数: {tries}）: {str(e)}")
            if tries < max_retries:
                logger.info(f"{delay_seconds}秒後にリトライします...")
                time.sleep(delay_seconds)
                delay_seconds *= 2  # 指数バックオフ

    # すべてのリトライが失敗した場合
    logger.error(f"AI応答生成失敗（{max_retries}回試行）: {str(last_error)}")
    raise last_error

def generate_ai_response_parallel(prompts_list, max_workers=2):
    """複数のプロンプトを並列で処理（仕様書準拠版）"""
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 各プロンプトを並列実行
        future_to_index = {
            executor.submit(generate_ai_response, prompt): i 
            for i, prompt in enumerate(prompts_list)
        }
        
        # 結果を元の順序で取得
        indexed_results = [None] * len(prompts_list)
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                result = future.result()
                indexed_results[index] = result
                logger.info(f"並列処理{index+1}完了: {len(result[0])} 文字, {result[1]:.2f}秒")
            except Exception as e:
                logger.error(f"並列処理エラー（インデックス {index}）: {str(e)}")
                indexed_results[index] = (f"エラーが発生しました: {str(e)}", 0)
    
    return indexed_results

def rate_limit_retry(max_retries=3, base_delay=1.0):
    """GA4 APIレート制限対応のデコレータ"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if "RATE_LIMIT_EXCEEDED" in str(e) or "429" in str(e):
                        if attempt < max_retries - 1:
                            # 指数バックオフ + ジッター
                            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                            logger.warning(f"レート制限エラー。{delay:.2f}秒後にリトライします...")
                            time.sleep(delay)
                            continue
                    raise e
            return None
        return wrapper
    return decorator

def create_error_response(status_code, message, request_id, details=None):
    """エラーレスポンス生成"""
    error_data = {
        "status": "error",
        "request_id": request_id,
        "error": {
            "code": status_code,
            "message": message
        }
    }
    if details:
        error_data["error"]["details"] = details

    return Response(
        json.dumps(error_data, ensure_ascii=False),
        status=status_code,
        mimetype='application/json; charset=utf-8'
    )

def save_to_cloud_storage(full_report, property_id, start_date, end_date, request_id, site_prompt="", cv_prompt=""):

    try:
        from google.cloud import storage
        import datetime

        # 保存先の情報を設定
        bucket_name = "matsumra_test"
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = f"ga4_report_{property_id}_{start_date}_to_{end_date}_{timestamp}_{request_id}.md"
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # プロンプト情報を含む完全なレポートを作成
        complete_report = full_report
        
        # プロンプト情報が提供されている場合は追加
        if site_prompt or cv_prompt:
            complete_report += "\n\n---\n\n# 分析に使用したプロンプト情報\n\n"
            
            if site_prompt:
                complete_report += "## サイト分析プロンプト\n\n```\n" + site_prompt + "\n```\n\n"
            
            if cv_prompt:
                complete_report += "## コンバージョン分析プロンプト\n\n```\n" + cv_prompt + "\n```\n\n"

        # MDレポートをUTF-8でエンコードしてアップロード
        blob.upload_from_string(
            complete_report,
            content_type='text/markdown; charset=utf-8'
        )
        logger.info(f"レポートをCloud Storageに保存しました: gs://{bucket_name}/{file_name}")

    except Exception as storage_error:
        logger.error(f"Cloud Storageへの保存中にエラーが発生しました: {str(storage_error)}")
        raise storage_error

def estimate_token_count(text, model="gpt-3.5-turbo"):
    """
    テキストのトークン数を推定する関数
    注: OpenAIのtiktokenを使用
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except Exception as e:
        logger.warning(f"トークン数推定エラー: {str(e)}")
        # フォールバック: 単語数で簡易推定（英語の場合の大まかな目安）
        return len(text.split()) / 0.75

def create_site_analysis_data(ga4_data):
    """サイト分析用のデータのみを抽出する関数（コンバージョンデータを除外）"""
    # 事前集計データが存在する場合は優先使用
    if "summary" in ga4_data:
        # 事前集計データのみを使用（詳細データは除外）
        site_data = {
            "p": ga4_data.get("p", ""),  # プロパティID
            "d": ga4_data.get("d", {}),  # 期間情報
            "summary": {
                "site": ga4_data["summary"].get("site", {}),
                "by_page": ga4_data["summary"].get("by_page", [])[:20],  # 上位20ページ
                "by_source": ga4_data["summary"].get("by_source", []),
                "by_device": ga4_data["summary"].get("by_device", []),
                "by_user_type": ga4_data["summary"].get("by_user_type", [])
            }
        }
        logger.info("サイト分析用データ: 事前集計データを使用")
    else:
        # 従来形式（詳細データ）
        site_data = {
            "p": ga4_data.get("p", ""),  # プロパティID
            "d": ga4_data.get("d", {}),  # 期間情報
            "h": ga4_data.get("h", []),  # ヘッダー
            "r": ga4_data.get("r", []),  # セッションデータ
        }
        logger.info("サイト分析用データ: 詳細データを使用（後方互換）")

    # データサイズをログ出力
    site_data_str = json.dumps(site_data, ensure_ascii=False)
    logger.info(f"サイト分析用データサイズ: {len(site_data_str)} 文字")

    return site_data

def create_conversion_analysis_data(ga4_data):
    """コンバージョン分析用のデータのみを抽出する関数"""
    # 事前集計データが存在する場合は優先使用
    if "summary" in ga4_data:
        # 事前集計コンバージョンデータのみを使用
        cv_data = {
            "p": ga4_data.get("p", ""),  # プロパティID
            "d": ga4_data.get("d", {}),  # 期間情報
            "summary": {
                "conversions": ga4_data["summary"].get("conversions", {}),
                "site": ga4_data["summary"].get("site", {}),  # サイト全体指標も含める
            }
        }
        logger.info("コンバージョン分析用データ: 事前集計データを使用")
    else:
        # 従来形式（詳細データ）- 最小限に抑制
        row_limit = 100
        limited_rows = ga4_data.get("r", [])[:row_limit] if len(ga4_data.get("r", [])) > row_limit else ga4_data.get("r", [])
        
        cv_data = {
            "p": ga4_data.get("p", ""),  # プロパティID
            "d": ga4_data.get("d", {}),  # 期間情報
            "c": ga4_data.get("c", {}),  # コンバージョン集計データ
            "dc": ga4_data.get("dc", []),  # 詳細コンバージョンデータ
            "h": ga4_data.get("h", []),  # ヘッダー（参照用）
            "r": limited_rows,  # 最小限のセッションデータ
        }
        logger.info(f"コンバージョン分析用データ: 詳細データを使用（後方互換）- 行数制限: {len(limited_rows)}/{len(ga4_data.get('r', []))}")

    # データサイズをログ出力
    cv_data_str = json.dumps(cv_data, ensure_ascii=False)
    logger.info(f"コンバージョン分析用データサイズ: {len(cv_data_str)} 文字")

    return cv_data

def create_ga4_site_analysis_prompt(ga4_data, analysis_type="basic", user_prompt=""):
    """サイト基本指標分析用のプロンプト生成（コンバージョン分析を除外）"""

    # サイト分析用のデータのみを抽出
    site_data = create_site_analysis_data(ga4_data)

    # データから分析期間を抽出
    start_date = site_data.get("d", {}).get("s", "")
    end_date = site_data.get("d", {}).get("e", "")

    # 実際のデータ範囲（行数制限に達した場合）
    actual_start = site_data.get("d", {}).get("actual_start", "")
    actual_end = site_data.get("d", {}).get("actual_end", "")
    limit_reached = ga4_data.get("limit_reached", False)

    # 制限に関する警告文を生成
    limit_warning = ""
    if limit_reached and (actual_start or actual_end):
        limit_warning = f"\n※注意: データ行数の制限に達したため、実際のデータ範囲は {actual_start} ～ {actual_end} です。この点を分析の際に考慮してください。"

    # 日数計算（日付がISO形式の場合）
    try:
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        days_count = (end_dt - start_dt).days + 1
    except (ValueError, TypeError):
        days_count = "不明"

    # ヘッダー説明の生成
    headers_explanation = """
    p=ページパス(pagePath), src=ソース(sessionSource), d=デバイス(deviceCategory), n=ユーザータイプ(newVsReturning), 
    v=PV数(screenPageViews), u=ユーザー数(totalUsers), nu=新規ユーザー数(newUsers), e=エンゲージメント率(engagementRate), 
    s=セッション数(sessions), a=平均セッション時間(averageSessionDuration), b=直帰率(bounceRate), ed=合計エンゲージメント時間(userEngagementDuration)

    ※重要: edは合計エンゲージメント時間のため、平均エンゲージメント時間を算出する場合は ed ÷ s (セッション数) で計算してください。
    """

    # ユーザープロンプトのセクションを追加
    user_prompt_section = ""
    if user_prompt:
        user_prompt_section = f"""
    【イベント説明と分析要望】
    {user_prompt}
    """

    # プロンプト内容を事前集計データに応じて変更
    if "summary" in site_data:
        # 事前集計データ用プロンプト
        data_instruction = """以下はGA4から取得した**事前集計済み**の正確な数値データです。これらの数値は既にGA4で計算済みのため、**絶対に再計算せず、そのまま使用**してください。"""
        analysis_note = """
    【重要】データについて:
    - summary.site: サイト全体の集計指標（正確な計算済み数値）
    - summary.by_page: ページ別集計（PV数順、上位20位）- ページパスフィルターが適用されている場合があります
    - summary.by_source: ソース別集計（セッション数順、上位20位）
    - summary.by_device: デバイス別集計
    - summary.by_user_type: 新規/リピーター別集計
    - summary.by_lp: 特定のLP（ランディングページ）別集計（PV・セッション・CVを含む）

    ※これらの数値は既にGA4で正確に計算されているため、再計算や推定は一切行わず、提示された数値をそのまま分析に使用してください。
    ※ページパスフィルターが適用されている場合、by_pageのデータは特定のパスに限定された結果です。
    ※LP集計データが存在する場合、各LPの詳細なパフォーマンス指標とコンバージョン率が含まれています。"""
    else:
        # 詳細データ用プロンプト（従来）
        data_instruction = """以下のGA4データを詳細に分析し、具体的な数値とその意味を解説した包括的なレポートを作成してください。"""
        analysis_note = f"""
    【データ構造説明】
    ヘッダー: {headers_explanation}"""

    # プロンプトの作成 - コンバージョン分析（観点7）を除外
    prompt = f"""
    あなたはGoogleアナリティクスの専門家データアナリストです。{data_instruction}なお、必ずMarkdown形式で作成してください。

    【分析期間】
    {start_date}から{end_date}までの{days_count}日間{limit_warning}

    【GA4データ】
    {json.dumps(site_data, ensure_ascii=False, indent=2)}{analysis_note}

    {user_prompt_section}

    【分析の観点】
    数値の合計は決して間違いないように慎重に算出し、出力はMDファイル形式の文章を用いること。
    1. 主要指標分析 :
    - サイト全体の主要パフォーマンス指標を提示して。
    - 各指標が示すサイトの健康状態、ユーザー行動の全体像について、数値と共に詳細に解説して。単なる数値の報告ではなく、それがポジティブ/ネガティブな傾向なのか、課題を示唆しているのか、背景にある可能性などを考察して。
    2. ページ別分析:
    - 取得された全ページのデータ（上位20位まで）を網羅的に分析してください。10件で省略せず、すべて表示してください。
    - ページパスフィルターが適用されている場合は、その旨を明記し、フィルター対象のページ群における分析であることを説明してください。
    - PV数上位ページだけでなく、エンゲージメント率が高い/低いページ、直帰率が高いページ、平均セッション時間が長い/短いページなど、特徴的なページを取り上げて。
    - 各ページのパフォーマンスを比較分析し、ユーザーに支持されているコンテンツ、あるいは改善が必要なコンテンツを特定して。
    - ページの役割（例：情報提供、回遊促進、コンバージョン誘導）と実際のユーザー行動（数値データ）を照らし合わせ、コンテンツの質や導線設計の課題、改善の方向性について具体的に論じて。
    3. 流入チャネル別分析:
    - `sessionSource` ごとの比較分析（上位20位まで）を網羅的に行い、すべてのソースを表示してください。
    - 各チャネルがサイト全体のパフォーマンスにどの程度貢献しているか（量と質の両面から）を評価して。
    - チャネルごとのユーザー行動特性（例：特定のチャネルからのユーザーはエンゲージメントが高いが滞在時間が短い、など）を明らかにし、その背景要因を推察して。
    - 各チャネルの強みと弱みを踏まえ、チャネルミックスの最適化や、特定のチャネルにおけるマーケティング施策の改善提案に繋げて。
    4. デバイス別分析 :
    - デバイスカテゴリ（デスクトップ、モバイル、タブレット）ごとの比較分析。
    - 各デバイスでのユーザー体験の質に差異がないか、数値データに基づいて評価して。
    - 特定のデバイスでパフォーマンスが低い場合、その原因（例：表示崩れ、操作性の問題、コンテンツの見づらさ等）を推察して。
    - デバイスごとの利用シーンやユーザーニーズの違いを考慮したコンテンツ戦略のヒントを提示して。
    5. ユーザータイプ別分析 :
        - 新規ユーザーとリピーターの行動の比較分析。
        - それぞれのユーザーセグメントがサイトにどのような価値をもたらしているか（例：新規ユーザーはリーチ拡大、リピーターはLTV向上）を考察して。
        - 新規ユーザーをリピーターに育成するための課題や、リピーターのエンゲージメントをさらに高めるための施策について、具体的なアイデアを提示して。
        - それぞれのユーザータイプがよく閲覧するコンテンツや利用する機能に違いがあれば、それも指摘して。
    6. LP（ランディングページ）別分析 :
        - 特定のLP（/entry04/, /entry05/, /entry06/など）ごとの詳細なパフォーマンス分析。
        - 各LPのPV数、セッション数、ユーザー数、エンゲージメント率、直帰率、平均セッション時間を比較分析。
        - 各LPのコンバージョン数とコンバージョン率を明示し、最も効果的なLPを特定。
        - LP間のパフォーマンス差異の原因分析（デザイン、コンテンツ、ターゲティング、流入元など）。
        - 改善が必要なLPの具体的な改善提案と、成功しているLPのベストプラクティス抽出。

    【レポートの書式】
    - タイトル：「GA4データ分析レポート ({start_date} - {end_date})」
    - 各セクションには見出しを付け、MDファイル形式の表形式と文章での解説を組み合わせてください.また、各解説は必ず非常に詳細に解説し、500文字以上生成しなさい。
    - **重要：表は省略せず、取得されたデータをすべて表示してください。特に上位20位のデータが提供されている場合は、10件で打ち切らず全件を表示すること。**
    - ページパスフィルターが適用されている場合は、レポートの冒頭でその旨を明記し、分析対象の範囲を明確にしてください。
    - LP集計データが存在する場合は、各LPの詳細な比較表を作成し、PV・セッション・CV・コンバージョン率を明示してください。
    - 表には数値データと共に、その意味や重要性も示してください
    - 分析の根拠となる具体的な数値を必ず含めてください
    - 数値計算は必ず正確に行い、誤りがないようにしてください。
    - エンゲージメント時間の計算について：詳細データの場合、edフィールドは「合計エンゲージメント時間」なので、平均を求める場合は ed ÷ s (セッション数) で計算してください。事前集計データの場合は既に平均値が提供されています。
    - 決して出力にはPythonコードを用いず、MDファイル形式の文章で表などを出力してください。

    【分析タイプ】
    {analysis_type}
    """

    # プロンプトのサイズとトークン数を計算してログに出力
    prompt_size = len(prompt)
    estimated_tokens = estimate_token_count(prompt)
    logger.info(f"サイト分析プロンプトサイズ: {prompt_size} 文字")
    logger.info(f"サイト分析推定トークン数: {estimated_tokens} トークン")

    # プロンプト内容をログに出力（デバッグ用）
    #logger.info(prompt)

    # トークン数が多い場合は警告
    if estimated_tokens > 30000:
        logger.warning(f"サイト分析プロンプトのトークン数が多すぎます（{estimated_tokens}）。Geminiの上限に近づいています。")

    return prompt

def create_ga4_conversion_analysis_prompt(ga4_data, analysis_type="conversion", user_prompt=""):
    """コンバージョン分析用のプロンプト生成（コンバージョン分析のみに特化）"""

    # コンバージョン分析用のデータのみを抽出
    conversion_data = create_conversion_analysis_data(ga4_data)

    # データから分析期間を抽出
    start_date = conversion_data.get("d", {}).get("s", "")
    end_date = conversion_data.get("d", {}).get("e", "")

    # 実際のデータ範囲（行数制限に達した場合）
    actual_start = conversion_data.get("d", {}).get("actual_start", "")
    actual_end = conversion_data.get("d", {}).get("actual_end", "")
    limit_reached = ga4_data.get("limit_reached", False)

    # 制限に関する警告文を生成
    limit_warning = ""
    if limit_reached and (actual_start or actual_end):
        limit_warning = f"\n※注意: データ行数の制限に達したため、実際のデータ範囲は {actual_start} ～ {actual_end} です。この点を分析の際に考慮してください。"

    # 日数計算（日付がISO形式の場合）
    try:
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        days_count = (end_dt - start_dt).days + 1
    except (ValueError, TypeError):
        days_count = "不明"

    # コンバージョンデータの抽出
    conversion_summary = json.dumps(conversion_data.get("c", {}), ensure_ascii=False, indent=2)
    detailed_conversions = json.dumps(conversion_data.get("dc", []), ensure_ascii=False, indent=2)

    # ユーザープロンプトのセクションを追加
    user_prompt_section = ""
    if user_prompt:
        user_prompt_section = f"""
    【イベント説明と分析要望】
    {user_prompt}
    """

    # プロンプト内容を事前集計データに応じて変更
    if "summary" in conversion_data:
        # 事前集計データ用プロンプト
        data_instruction = """以下はGA4から取得した**コンバージョンの事前集計済み**正確な数値データです。これらの数値は既にGA4で計算済みのため、**絶対に再計算せず、そのまま使用**してください。"""
        data_structure_note = """
    【重要】データについて:
    - summary.conversions: コンバージョンの各種集計データ（正確な計算済み数値）
    - total_by_event: イベント別総数
    - by_page: ページ別コンバージョン数（ページパスフィルターが適用されている場合があります）
    - by_source: ソース別コンバージョン数
    - by_device: デバイス別コンバージョン数
    - by_hour: 時間帯別コンバージョン数
    - summary.site: サイト全体指標（コンバージョン率計算用）
    - summary.by_lp: 特定のLP（ランディングページ）別集計（PV・セッション・CVを含む）

    ※これらの数値は既にGA4で正確に計算されているため、再計算や推定は一切行わず、提示された数値をそのまま分析に使用してください。
    ※ページパスフィルターが適用されている場合、by_pageのデータは特定のパスに限定された結果です。
    ※LP集計データが存在する場合、各LPの詳細なパフォーマンス指標とコンバージョン率が含まれています。"""
    else:
        # 詳細データ用プロンプト（従来）
        data_instruction = """以下のGA4データからコンバージョンに関する詳細な分析を行い、具体的な数値とその意味を解説した包括的なレポートを作成してください。"""
        data_structure_note = """
    【コンバージョンデータ構造】
    このデータにはコンバージョン情報が含まれています：
    - c: 日付・イベント別のコンバージョン集計
    - dc: コンバージョンの詳細情報（ページ、メディア、デバイス、ランディングページ、時間帯など）"""

    # プロンプトの作成 - コンバージョン分析（観点7）のみ
    prompt = f"""
    あなたはGoogleアナリティクスの専門家データアナリストです。{data_instruction}

    【分析期間】
    {start_date}から{end_date}までの{days_count}日間{limit_warning}{data_structure_note}
    - h, r: サイト全体のデータ（参考情報として限定的に含む）

    【時間データの形式について】
    コンバージョンの時間情報は「hour」フィールドに「YYYYMMDDHH」形式で格納されています。
    例：
    - 「2023060113」は「2023年6月1日13時台」を意味します
    - 「2023060210」は「2023年6月2日10時台」を意味します
    時間帯分析を行う際は、この形式からHH（時間）部分を抽出し、0時〜23時の各時間帯ごとに集計してください。

    【コンバージョン情報】
    集計データ:
    {json.dumps(conversion_data, ensure_ascii=False, indent=2)}
    {user_prompt_section}

    【分析の観点】
    7. コンバージョン分析：以下の観点でコンバージョンを詳細分析してください
    a. コンバージョンサマリー：全体CVR、イベント別コンバージョン数・割合を明示
    b. ページパス分析：コンバージョンが発生したページの詳細分析と考察（上位20位まですべて表示）
        - ページパスフィルターが適用されている場合は、その旨を明記し、フィルター対象のページ群におけるコンバージョン分析であることを説明してください
    c. セッションソース分析：流入元別のコンバージョン状況と各ソースの効率性分析（上位20位まですべて表示）
    d. デバイスカテゴリ分析：デバイス別のコンバージョン状況と最適化ポイント
    e. ランディングページ分析：コンバージョンにつながったランディングページの貢献度評価
    f. 時間帯分析：コンバージョンが発生した時間帯の傾向と特徴
        - 0時〜23時の各時間帯ごとのコンバージョン数を集計
        - 時間帯別のコンバージョン率を算出（可能な場合）
        - ピーク時間帯を特定し、ユーザー行動の考察を追加
    g. クロス分析：上記a〜fの要素を掛け合わせた多角的分析（例：デバイス×時間帯、ソース×ランディングページなど）
        h. コンバージョン経路：流入からコンバージョンまでの主要パスと最適化ポイント
    i. LP別コンバージョン分析：特定のLP（/entry04/, /entry05/, /entry06/など）ごとのコンバージョン状況
        - 各LPのコンバージョン数、コンバージョン率、イベント別コンバージョン数を明示
        - LP間のコンバージョン効率比較とランキング
        - 高コンバージョンLPの成功要因分析
        - 低コンバージョンLPの改善ポイント特定
    8. 総括と提案：分析結果に基づいた改善施策を提案してください

    【レポートの書式】
    - タイトル：「GA4コンバージョン分析レポート ({start_date} - {end_date})」
    - 各セクションには見出しを付け、表形式と文章での解説を組み合わせてください.各解説は必ず非常に詳細に行い、500文字以上解説しなさい。
    - **重要：表は省略せず、取得されたコンバージョンデータをすべて表示してください。上位20位のデータが提供されている場合は、10件で打ち切らず全件を表示すること。**
    - ページパスフィルターが適用されている場合は、レポートの冒頭でその旨を明記し、分析対象の範囲を明確にしてください。
    - LP集計データが存在する場合は、各LPの詳細な比較表を作成し、PV・セッション・CV・コンバージョン率を明示してください。
    - 表には数値データと共に、その意味や重要性も示してください
    - 改善提案部分では具体的なアクションアイテムを挙げてください
    - 分析の根拠となる具体的な数値を必ず含めてください

    【コンバージョン分析の注意点】
    - 各CV分析（ページパス、ソース、デバイス、ランディングページ、時間帯）は必ず実施してください
    - データが少ない場合でも、「データが少ないため分析が限定的」などと記載し、拒否しないでください
    - 時間帯分析では、コンバージョンのピーク時間帯とその理由について考察を加えてください
    - CVデータが存在する限り、たとえサンプル数が少なくても分析を行ってください
    - 各ソース・デバイス・ランディングページの効率を示すCVRなどの指標も計算して明示してください
    - 数値計算は必ず正確に行い、誤りがないようにしてください。
    - エンゲージメント時間の計算について：詳細データが含まれる場合、エンゲージメント時間は合計値なので、平均を求める場合はセッション数で除算してください。
    - 決してPythonコードを生成しないでください。

    【分析タイプ】
    {analysis_type}
    """

    # プロンプトのサイズとトークン数を計算してログに出力
    prompt_size = len(prompt)
    estimated_tokens = estimate_token_count(prompt)
    logger.info(f"コンバージョン分析プロンプトサイズ: {prompt_size} 文字")
    logger.info(f"コンバージョン分析推定トークン数: {estimated_tokens} トークン")

    # プロンプト内容をログに出力（デバッグ用）
    #logger.info(prompt)

    # トークン数が多い場合は警告
    if estimated_tokens > 30000:
        logger.warning(f"コンバージョン分析プロンプトのトークン数が多すぎます（{estimated_tokens}）。Geminiの上限に近づいています。")

    return prompt

def merge_analysis_reports(site_report, cv_report):
    """サイト分析レポートとコンバージョン分析レポートを統合"""

    logger.info("分析レポートの統合を開始...")

    # コンバージョンレポートから「## 7」や「## コンバージョン分析」の部分を抽出
    cv_section_start = None
    cv_pattern = r'(?:\n|^)##\s*(?:7|[７7][\s\.]*|コンバージョン分析)'
    cv_match = re.search(cv_pattern, cv_report, re.MULTILINE)

    if cv_match:
        cv_section_start = cv_match.start()
        logger.info(f"コンバージョン分析セクションを見つけました: インデックス {cv_section_start}")
        cv_content = cv_report[cv_section_start:]
    else:
        # 見出しが見つからない場合は全体を使用
        logger.warning("コンバージョン分析の見出しが見つかりませんでした。全体を使用します。")
        cv_content = cv_report

    # サイトレポートから「## 7」や「## コンバージョン分析」などがあれば、その直前までを使用
    site_section_end = None
    site_match = re.search(cv_pattern, site_report, re.MULTILINE)

    if site_match:
        site_section_end = site_match.start()
        logger.info(f"サイトレポートでコンバージョン分析セクションの開始位置を見つけました: インデックス {site_section_end}")
        site_content = site_report[:site_section_end]
    else:
        # 見出しが見つからない場合は全体を使用
        logger.info("サイトレポートにコンバージョン分析の見出しがありませんでした。全体を使用します。")
        site_content = site_report

    # 「## 8」や「## 総括と提案」を探す
    site_summary_match = re.search(r'(?:\n|^)##\s*(?:8|[８8][\s\.]*|総括と提案)', site_report, re.MULTILINE)
    cv_summary_match = re.search(r'(?:\n|^)##\s*(?:8|[８8][\s\.]*|総括と提案)', cv_report, re.MULTILINE)

    summary_section = ""

    # コンバージョンレポートの方が総括が詳しい可能性が高いので優先
    if cv_summary_match:
        logger.info("コンバージョンレポートから総括セクションを使用します")
        summary_start = cv_summary_match.start()
        summary_section = cv_report[summary_start:]
        # コンバージョンコンテンツから総括部分を削除（重複防止）
        cv_content = cv_content[:summary_start]
    elif site_summary_match:
        logger.info("サイトレポートから総括セクションを使用します")
        summary_start = site_summary_match.start()
        summary_section = site_report[summary_start:]
        # サイトコンテンツから総括部分を削除（重複防止）
        site_content = site_content[:summary_start]

    # 最終レポートの結合
    merged_report = site_content.strip() + "\n\n" + cv_content.strip()

    # 総括セクションがあれば追加
    if summary_section:
        merged_report += "\n\n" + summary_section.strip()

    logger.info(f"レポート統合完了: サイト分析 {len(site_content)} 文字 + コンバージョン分析 {len(cv_content)} 文字 + 総括 {len(summary_section)} 文字 = 合計 {len(merged_report)} 文字")

    return merged_report

def get_credentials():
    """サービスアカウントの認証情報を取得する"""
    try:
        creds_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if not creds_json:
            logger.error("GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set")
            raise Exception("認証情報が設定されていません")

        try:
            # 環境変数からJSONを読み込む
            creds_dict = json.loads(creds_json)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse credentials JSON: {str(e)}")
            raise Exception("認証情報のJSONパースに失敗しました")

        # 一時ファイルにJSONを書き出す
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                json.dump(creds_dict, temp_file)
                temp_file_path = temp_file.name
        except IOError as e:
            logger.error(f"Failed to write credentials to temp file: {str(e)}")
            raise Exception("認証情報の一時ファイル作成に失敗しました")

        try:
            # 認証情報を作成
            credentials = service_account.Credentials.from_service_account_file(
                temp_file_path,
                scopes=['https://www.googleapis.com/auth/analytics.readonly']
            )
        except Exception as e:
            logger.error(f"Failed to create credentials: {str(e)}")
            raise Exception("認証情報の作成に失敗しました")
        finally:
            # 一時ファイルを削除
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                logger.warning(f"Failed to delete temp file: {str(e)}")

        return credentials
    except Exception as e:
        logger.error(f"Error in get_credentials: {str(e)}\n{traceback.format_exc()}")
        raise

@functions_framework.http
def process_property_data(request) -> Dict[str, Any]:
    """
    GA4のプロパティIDと日付に基づいてデータを取得し、Difyに返却する関数
    """
    try:
        # リクエストの詳細をログ出力
        logger.info(f"Received request: {request.method} {request.path}")
        logger.info(f"Headers: {dict(request.headers)}")
        logger.info(f"Query params: {dict(request.args)}")
        
        # リクエストデータの取得（force=True でContent-Typeに関係なくJSONとして解析を試みる）
        request_json = {}
        try:
            # まずJSONボディを試す
            if request.is_json or request.content_type == 'application/json':
                body_json = request.get_json(force=True)
                if body_json and isinstance(body_json, dict):
                    request_json = body_json
                    logger.info(f"Request body JSON: {json.dumps(request_json, ensure_ascii=False)}")
                    
                    # 特殊なケース: requestキーに文字列としてHTTPリクエスト全体が入っている場合
                    if 'request' in request_json and isinstance(request_json['request'], str):
                        request_str = request_json['request']
                        logger.info(f"Found 'request' key with HTTP request string: {request_str[:100]}...")
                        
                        # リクエストボディ部分を抽出（空行の後の部分）
                        try:
                            body_start = request_str.find('\r\n\r\n')
                            if body_start > -1:
                                body_str = request_str[body_start + 4:]
                                logger.info(f"Extracted body from request string: {body_str}")
                                
                                # JSONデータとして解析
                                try:
                                    body_data = json.loads(body_str)
                                    if isinstance(body_data, dict):
                                        logger.info(f"Successfully parsed inner request body: {json.dumps(body_data, ensure_ascii=False)}")
                                        # 元のrequest_jsonを置き換え
                                        request_json = body_data
                                except json.JSONDecodeError as je:
                                    logger.warning(f"Failed to parse inner request body as JSON: {str(je)}")
                        except Exception as e:
                            logger.warning(f"Error extracting body from request string: {str(e)}")
            
            # JSONボディが空の場合やJSONでない場合は、フォームデータを試す
            if not request_json and request.form:
                logger.info("Trying to get data from form")
                for key in request.form:
                    request_json[key] = request.form[key]
                logger.info(f"Request form data: {json.dumps(request_json, ensure_ascii=False)}")
            
            # クエリパラメータを確認（優先度は低い）
            if request.args:
                logger.info("Found query parameters")
                # URLクエリパラメータがある場合、JSONボディに追加（JSONボディが優先）
                for key in request.args:
                    if key not in request_json:  # JSONボディで既に定義されていない場合のみ
                        request_json[key] = request.args[key]
                
            # リクエストの詳細な内容をログに出力
            logger.info(f"Processed request data: {json.dumps(request_json, ensure_ascii=False)}")
            logger.info(f"Request type: {type(request_json)}")
            logger.info(f"Request keys: {list(request_json.keys())}")
            
        except Exception as e:
            logger.error(f"Failed to parse request data: {str(e)}")
            # クエリパラメータだけでも試してみる
            if request.args:
                request_json = dict(request.args)
                logger.info(f"Falling back to query parameters: {json.dumps(request_json, ensure_ascii=False)}")
            else:
                response_data = {
                    "error": "リクエストデータの解析に失敗しました",
                    "detail": str(e)
                }
                return Response(
                    json.dumps(response_data, ensure_ascii=False, separators=(',', ':')),
                    status=400,
                    mimetype='application/json; charset=utf-8'
                )
        
        if not request_json:
            logger.error("Empty request body")
            response_data = {"error": "リクエストボディが空です"}
            return Response(
                json.dumps(response_data, ensure_ascii=False, separators=(',', ':')),
                status=400,
                mimetype='application/json; charset=utf-8'
            )
        
        # 必要なパラメータのみを抽出
        try:
            property_id = str(request_json.get("property_id"))  # 数値の場合も文字列に変換
            start_date = request_json.get("start_date")  # 新パラメータ
            date = request_json.get("date", "")  # 旧パラメータも後方互換のため維持
            
            # 日付パラメータの優先順位処理
            if date and not start_date:  # 旧形式の場合
                start_date = date
            
            end_date = request_json.get("end_date", start_date)  # 省略時はstart_dateと同じ
            
            cv_events = request_json.get("cv", "click_lpo")  # cvパラメータの取得、デフォルトはclick_lpo
            
            # 【新規追加】ページパスフィルタリング用パラメータの取得
            path_filter = request_json.get("path", "")  # pathパラメータの取得、デフォルトは空文字（フィルターなし）
            
            # sys.* パラメータは無視
            logger.info(f"Extracted parameters - property_id: {property_id}, start_date: {start_date}, end_date: {end_date}, cv_events: {cv_events}, path_filter: {path_filter}")
            
            # パラメータのバリデーション
            if not all([property_id, start_date]):
                logger.error(f"Missing required parameters - property_id: {property_id}, start_date: {start_date}")
                response_data = {
                    "error": "必須パラメータが不足しています"
                }
                return Response(
                    json.dumps(response_data, ensure_ascii=False, separators=(',', ':')),
                    status=400,
                    mimetype='application/json; charset=utf-8'
                )
        except Exception as e:
            logger.error(f"Error processing request parameters: {str(e)}")
            response_data = {
                "error": "パラメータの処理中にエラーが発生しました",
                "detail": str(e)
            }
            return Response(
                json.dumps(response_data, ensure_ascii=False, separators=(',', ':')),
                status=400,
                mimetype='application/json; charset=utf-8'
            )
            
        # データの取得
        data = fetch_ga4_data(property_id, start_date, end_date, cv_events, path_filter=path_filter)
        
        logger.info(f"Successfully retrieved data for property_id: {property_id}")
        response_data = {
            "status": "success",
            "data": data
        }
        
        # JSONの圧縮（スペースなし）
        return Response(
            json.dumps(response_data, ensure_ascii=False, separators=(',', ':')),
            status=200,
            mimetype='application/json; charset=utf-8'
        )
        
    except Exception as e:
        logger.error(f"Error in process_property_data: {str(e)}\n{traceback.format_exc()}")
        response_data = {
            "error": str(e),
            "detail": traceback.format_exc()
        }
        return Response(
            json.dumps(response_data, ensure_ascii=False, separators=(',', ':')),
            status=500,
            mimetype='application/json; charset=utf-8'
        )

@functions_framework.http
def analyze_ga4_data(request):
    """GA4データ分析メイン関数（エラーハンドリング強化版）"""
    request_id = create_request_id()
    request_logger = setup_request_logger(request_id)

    try:
        request_logger.info("リクエスト処理開始")
        
        # リクエストデータの取得
        request_json = request.get_json(force=True) if request.is_json else {}
        
        # パラメータの抽出
        property_id = str(request_json.get("property_id", ""))
        start_date = request_json.get("start_date", "")
        date = request_json.get("date", "")  # 後方互換性のため
        
        # 日付パラメータの処理
        if date and not start_date:
            start_date = date
            
        end_date = request_json.get("end_date", start_date)
        cv_events = request_json.get("cv", "click_lpo")
        analysis_type = request_json.get("analysis_type", "comprehensive")
        prompt_text = request_json.get("prompt", "")
        path_filter = request_json.get("path", "")
        row_limit = request_json.get("row_limit", 11000)
        
        if isinstance(row_limit, str) and row_limit.isdigit():
            row_limit = int(row_limit)
        
        # 必須パラメータのチェック
        if not all([property_id, start_date]):
            request_logger.error("必須パラメータが不足しています")
            return create_error_response(400, "必須パラメータが不足しています", request_id)
        
        # GA4データ取得
        try:
            request_logger.info("GA4データ取得開始")
            start_time = time.time()
            ga4_data = fetch_ga4_data_with_retry(property_id, start_date, end_date, cv_events, row_limit, path_filter)
            ga4_fetch_time = time.time() - start_time
            request_logger.info(f"GA4データ取得完了: {ga4_fetch_time:.2f}秒")
        except Exception as ga4_error:
            request_logger.error(f"GA4データ取得エラー: {str(ga4_error)}")
            return create_error_response(500, "GA4データ取得エラー", request_id, str(ga4_error))
        
        # AI分析実行
        try:
            request_logger.info("AI分析開始")
            prompts_list = [
                create_ga4_site_analysis_prompt(ga4_data, analysis_type, prompt_text),
                create_ga4_conversion_analysis_prompt(ga4_data, analysis_type, prompt_text)
            ]
            results = generate_ai_response_parallel(prompts_list, max_workers=2)
            
            site_report, site_time = results[0]
            cv_report, cv_time = results[1]
            full_report = merge_analysis_reports(site_report, cv_report)
            
            # 並列実行のため、最大実行時間を使用
            total_time = ga4_fetch_time + max(site_time, cv_time)
            request_logger.info(f"AI分析完了: 合計{total_time:.2f}秒")
        except Exception as ai_error:
            request_logger.error(f"AI分析エラー: {str(ai_error)}")
            return create_error_response(500, "AI分析エラー", request_id, str(ai_error))
        
        # Cloud Storage保存
        try:
            save_to_cloud_storage(full_report, property_id, start_date, end_date, request_id, site_prompt=prompts_list[0], cv_prompt=prompts_list[1])
        except Exception as storage_error:
            request_logger.warning(f"Cloud Storage保存エラー: {str(storage_error)}")
            # 保存エラーは致命的ではないため、処理を継続
        
        # 成功レスポンス
        return Response(
            json.dumps({
                "status": "success",
                "report": full_report,
                "request_id": request_id,
                "execution_time": f"{total_time:.2f}秒"
            }, ensure_ascii=False),
            status=200,
            mimetype='application/json; charset=utf-8'
        )
        
    except Exception as e:
        request_logger.error(f"予期しないエラー: {str(e)}")
        return create_error_response(500, "サーバー内部エラー", request_id, str(e))

# GA4事前集計用のリクエスト生成関数

# データ処理関数
def process_site_summary(response):
    """サイト全体集計レスポンスを処理"""
    if not response.rows:
        return {
            "total_users": 0,
            "new_users": 0,
            "sessions": 0,
            "page_views": 0,
            "engagement_rate": 0.0,
            "avg_session_duration": 0.0,
            "bounce_rate": 0.0,
            "avg_engagement_duration": 0.0
        }

    row = response.rows[0]

    # 基本指標を取得
    total_users = int(row.metric_values[0].value) if row.metric_values[0].value else 0
    new_users = int(row.metric_values[1].value) if row.metric_values[1].value else 0
    sessions = int(row.metric_values[2].value) if row.metric_values[2].value else 0
    page_views = int(row.metric_values[3].value) if row.metric_values[3].value else 0
    engagement_rate = float(row.metric_values[4].value) if row.metric_values[4].value else 0.0
    avg_session_duration = float(row.metric_values[5].value) if row.metric_values[5].value else 0.0
    bounce_rate = float(row.metric_values[6].value) if row.metric_values[6].value else 0.0
    total_engagement_duration = float(row.metric_values[7].value) if row.metric_values[7].value else 0.0

    # 平均エンゲージメント時間を計算（合計エンゲージメント時間 ÷ セッション数）
    avg_engagement_duration = 0.0
    if sessions > 0:
        avg_engagement_duration = total_engagement_duration / sessions

    return {
        "total_users": total_users,
        "new_users": new_users,
        "sessions": sessions,
        "page_views": page_views,
        "engagement_rate": engagement_rate,
        "avg_session_duration": avg_session_duration,
        "bounce_rate": bounce_rate,
        "avg_engagement_duration": round(avg_engagement_duration, 2),
    }

@rate_limit_retry(max_retries=3, base_delay=1.0)
def fetch_ga4_data_with_retry(property_id, start_date, end_date, cv_events, row_limit, path_filter):
    """GA4データ取得（レート制限対応版）"""
    return fetch_ga4_data(property_id, start_date, end_date, cv_events, row_limit, path_filter)

def fetch_ga4_data(property_id: str, start_date: str, end_date: str, cv_events: str = "click_lpo", row_limit: int = 11000, path_filter: str = "") -> Dict[str, Any]:
    """
    GA4からデータを取得する関数

    Args:
        property_id (str): GA4のプロパティID（例: "262336548"）
        start_date (str): 開始日（YYYY-MM-DD形式）
        end_date (str): 終了日（YYYY-MM-DD形式）
        cv_events (str): コンバージョンとして扱うイベント名（複数の場合は/で区切り）
        row_limit (int): 取得する最大行数
        path_filter (str): ページパスフィルター（カンマ区切りで複数指定可能、例: "/,/category/gtm-click/,/aboutus/,/works/"）

    Returns:
        Dict[str, Any]: 取得したデータ
    """
    try:
        # 認証情報を取得
        credentials = get_credentials()
        logger.info("Successfully obtained credentials")
        
        # GA4クライアントの初期化
        client = BetaAnalyticsDataClient(credentials=credentials)
        
        # プロパティIDの処理
        if property_id.startswith('properties/'):
            property_id = property_id.replace('properties/', '')
        
        logger.info(f"Sending request to GA4 API for property_id: {property_id}")
        
        # --- 修正版 start ---
        # path_filterが指定されている場合はカンマ区切りで分割し、EXACT一致でフィルタする
        if path_filter:
            path_list = [p.strip() for p in path_filter.split(',') if p.strip()]
        else:
            path_list = None

        # 集計データの取得（lp_pathsは廃止してpath_filterをそのまま使う）
        summary_data = fetch_summary_data(
            client,
            property_id,
            start_date,
            end_date,
            cv_events,
            path_filter=path_filter
        )
        logger.info("集計データの処理完了")
        # --- 修正版 end ---

        
        # 事前集計データの取得
        logger.info("事前集計データの取得を開始...")
        
        # データ行数の制限を設定
        row_limit = 10000  # デフォルト値を設定
        # GA4データ取得行数制限をログ出力
        logger.info(f"GA4データ取得行数制限: {row_limit}行")

        # page_path_filters の初期化、設定
        page_path_filters = []  
        if path_filter: 
            path_list = [p.strip() for p in path_filter.split(',') if p.strip()] # カンマで分割し、前後の空白を除去
            if path_list:
                for single_path_value in path_list:
                    path_expression = FilterExpression(
                        filter=Filter(
                            field_name="pagePath", 
                            string_filter=Filter.StringFilter(
                                value=single_path_value,
                                match_type=Filter.StringFilter.MatchType.EXACT 
                            )
                        )
                    )
                    page_path_filters.append(path_expression)
                logger.info(f"生成されたページパスフィルター: {len(page_path_filters)}件")
            else:
                logger.info("path_filter は指定されましたが、有効なパスが見つかりませんでした。")
        else:
            logger.info("path_filter なし")


        # セッションデータのリクエスト（ページパスフィルター適用）
        session_request_params = {
            "property": f"properties/{property_id}",
            "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
            "dimensions": [
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
                Dimension(name="deviceCategory"),
                Dimension(name="newVsReturning"),
            ],
            "metrics": [
                Metric(name="screenPageViews"),
                Metric(name="totalUsers"),
                Metric(name="newUsers"),
                Metric(name="engagementRate"),
                Metric(name="sessions"),
                Metric(name="averageSessionDuration"),
                Metric(name="bounceRate"),
                Metric(name="userEngagementDuration"),
            ],
            "limit": row_limit
        }

        # ページパスフィルターが指定されている場合は適用
        if page_path_filters:
            if len(page_path_filters) == 1:
                # 単一パスの場合
                session_request_params["dimension_filter"] = page_path_filters[0]
            else:
                # 複数パスの場合（OR条件）
                session_request_params["dimension_filter"] = FilterExpression(
                    or_group=FilterExpressionList(expressions=page_path_filters)
                )
            logger.info("セッションデータリクエストにページパスフィルターを適用しました")

        session_request = RunReportRequest(**session_request_params)
        
        # レポートの実行
        session_response = client.run_report(session_request)
        
        logger.info(f"Received session response from GA4 API with {len(session_response.rows)} rows")
        
        # コンバージョンデータのリクエスト
        event_names = cv_events.split('/')
        
        # イベント名フィルターの構築
        if len(event_names) == 1:
            # 単一イベントの場合
            cv_event_filter = Filter(
                field_name='eventName',
                string_filter=Filter.StringFilter(value=event_names[0], match_type=Filter.StringFilter.MatchType.EXACT)
            )
            event_filters = [FilterExpression(filter=cv_event_filter)]
        else:
            # 複数イベントの場合（OR条件）
            filter_expressions = []
            for event_name in event_names:
                event_filter = Filter(
                    field_name='eventName',
                    string_filter=Filter.StringFilter(value=event_name.strip(), match_type=Filter.StringFilter.MatchType.EXACT)
                )
                filter_expressions.append(FilterExpression(filter=event_filter))
            
            # OR条件でフィルターを構築
            event_filters = [FilterExpression(or_group=FilterExpressionList(expressions=filter_expressions))]

        # pathフィルターはセッション分析のみに適用し、コンバージョン分析には適用しない
        combined_filters = event_filters
        # if page_path_filters:
        #     if len(page_path_filters) == 1:
        #         combined_filters.append(page_path_filters[0])
        #     else:
        #         combined_filters.append(FilterExpression(or_group=FilterExpressionList(expressions=page_path_filters)))
        #     logger.info("コンバージョンデータリクエストにページパスフィルターを適用しました")

        conversion_request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="date"),
                Dimension(name="eventName"),
                Dimension(name="pagePath"),        # ページパスを追加
                Dimension(name="sessionSource"),   # セッションソースを追加
                Dimension(name="deviceCategory"),  # デバイスカテゴリを追加
                Dimension(name="landingPage"),     # ランディングページを追加
                Dimension(name="dateHour"),        # 時間帯情報を追加
            ],
            metrics=[
                Metric(name="eventCount"),
            ],
            dimension_filter=FilterExpression(and_group=FilterExpressionList(expressions=combined_filters)),
            limit=row_limit
        )

        conversion_response = client.run_report(conversion_request)
        logger.info(f"Received conversion response from GA4 API with {len(conversion_response.rows)} rows")
        
        # 行数リミットに達したかどうかのフラグ
        limit_reached = len(session_response.rows) >= row_limit
        
        # 実際に取得したデータの日付範囲を確認（データが存在する場合）
        actual_date_range = {"s": start_date, "e": end_date}
        conversion_dates = set()
        
        # コンバージョンデータをイベントごとにマッピング
        conversion_data = {}
        detailed_conversions = []  # 詳細なコンバージョンデータを格納する配列
        
        for row in conversion_response.rows:
            date = row.dimension_values[0].value
            conversion_dates.add(date)
            event_name = row.dimension_values[1].value
            page_path = row.dimension_values[2].value if len(row.dimension_values) > 2 else ""
            session_source = row.dimension_values[3].value if len(row.dimension_values) > 3 else ""
            device_category = row.dimension_values[4].value if len(row.dimension_values) > 4 else ""
            landing_page = row.dimension_values[5].value if len(row.dimension_values) > 5 else ""
            date_hour = row.dimension_values[6].value if len(row.dimension_values) > 6 else ""
            event_count = int(row.metric_values[0].value) if row.metric_values[0].value else 0
            
            # 日付ごとのコンバージョン集計（既存の処理）
            if date not in conversion_data:
                conversion_data[date] = {}
            if event_name not in conversion_data[date]:
                conversion_data[date][event_name] = 0
            conversion_data[date][event_name] += event_count
            
            # 詳細なコンバージョンデータを追加
            detailed_conversions.append({
                "date": date,
                "event": event_name,
                "path": page_path,
                "source": session_source,
                "device": device_category,
                "landing": landing_page,     # ランディングページを追加
                "hour": date_hour,           # 時間帯情報を追加
                "count": event_count
            })
        
        # 全体のコンバージョンデータを集計（日付ごとではなく全体）
        all_conversions = {}
        for date_conversions in conversion_data.values():
            for event_name, count in date_conversions.items():
                if event_name not in all_conversions:
                    all_conversions[event_name] = 0
                all_conversions[event_name] += count

        # レスポンスデータの整形
        formatted_data = []
        for row in session_response.rows:
            try:
                # ディメンションの安全な取得（値が存在しない場合は空文字を返す）
                def get_dimension_value(index):
                    try:
                        return row.dimension_values[index].value
                    except (IndexError, AttributeError):
                        return ""

                # メトリクスの安全な取得（値が存在しない場合はデフォルト値を返す）
                def get_metric_value(index, default_value=0, convert_type=int):
                    try:
                        value = row.metric_values[index].value
                        if value == "":  # 空文字の場合
                            return default_value
                        return convert_type(value)
                    except (IndexError, AttributeError, ValueError):
                        return default_value

                # 浮動小数点値を丸める関数
                def round_float(value, decimals=2):
                    if isinstance(value, float):
                        return round(value, decimals)
                    return value
                
                # ページタイトルを制限する
                page_title = get_dimension_value(1)
                if len(page_title) > 40:
                    page_title = page_title[:40] + "..."
                
                # フィールド名を短縮し、すべてのデータを保持（cnvフィールドを削除）
                item = {
                    "path": get_dimension_value(0),
                    "src": get_dimension_value(1),
                    "dev": get_dimension_value(2),
                    "nvr": get_dimension_value(3),
                    "pv": get_metric_value(0, default_value=0, convert_type=int),  # page_views
                    "usr": get_metric_value(1, default_value=0, convert_type=int),  # users
                    "nusr": get_metric_value(2, default_value=0, convert_type=int),  # new_users
                    "er": round_float(get_metric_value(3, default_value=0.0, convert_type=float)),  # engagement_rate
                    "ses": get_metric_value(4, default_value=0, convert_type=int),  # sessions
                    "asd": round_float(get_metric_value(5, default_value=0.0, convert_type=float)),  # avg_session_duration
                    "br": round_float(get_metric_value(6, default_value=0.0, convert_type=float)),  # bounce_rate
                    "engd": round_float(get_metric_value(7, default_value=0.0, convert_type=float)),  # userEngagementDuration
                }
                
                formatted_data.append(item)
            except Exception as e:
                logger.warning(f"Error processing row: {str(e)}")
                continue
        
        # 実際のデータの日付範囲を更新（データがある場合のみ）
        if session_response.rows:
            actual_date_range["actual_start"] = session_response.rows[0].dimension_values[0].value
            actual_date_range["actual_end"] = session_response.rows[-1].dimension_values[0].value
        
        # トークン最適化形式に変換
        optimized_data = format_data_token_optimized(formatted_data)
        
        # 最適化されたレスポンス形式を返す（事前集計データを追加）
        return {
            "p": property_id,  # property_id を短縮
            "d": actual_date_range,  # 期間情報を短縮（実際の日付範囲も含む）
            "c": all_conversions,  # コンバージョンデータ
            "h": optimized_data["h"],  # ヘッダー定義
            "r": optimized_data["r"],   # 行データ（配列のみ）
            "dc": detailed_conversions,  # 詳細なコンバージョンデータを追加
            "limit_reached": limit_reached,
            "summary": summary_data  # 事前集計データを追加
        }
        
    except Exception as e:
        logger.error(f"Error in fetch_ga4_data: {str(e)}\n{traceback.format_exc()}")
        raise Exception(f"GA4データ取得中にエラーが発生しました: {str(e)}")

def format_data_token_optimized(formatted_data):
    """
    データをトークン最適化された列指向形式に変換する関数

    Args:
        formatted_data (List[Dict]): 元の形式のデータ
        
    Returns:
        Dict: 最適化された形式のデータ
    """
    # ヘッダー定義（一文字キー）
    # 対応：p=path（ページパス）, src=src（ソース）, d=dev（デバイス）, n=nvr（新規/リピーター）,
    #     v=pv（ページビュー）, u=usr（ユーザー数）, nu=nusr（新規ユーザー）, e=er（エンゲージメント率）,
    #     s=ses（セッション数）, a=asd（平均セッション時間）, b=br（直帰率）, ed=engd（エンゲージメント時間）
    headers = ["p", "src", "d", "n", "v", "u", "nu", "e", "s", "a", "b", "ed"]

    # データ行を配列のみの構造に変換
    row_data = []
    for item in formatted_data:
        row_values = [
            item["path"], item["src"], item["dev"], item["nvr"],
            item["pv"], item["usr"], item["nusr"], item["er"],
            item["ses"], item["asd"], item["br"], item["engd"]
        ]
        row_data.append(row_values)

    return {
        "h": headers,  # ヘッダー（プロパティキー）配列
        "r": row_data  # データ行の配列
    }


def parse_query_params(url):
    """URLからクエリパラメータを抽出する関数"""
    try:
        if not url:
            return {}
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        # 各パラメータの最初の値のみを取得（リストから値に変換）
        return {k: v[0] if v and len(v) > 0 else '' for k, v in query_params.items()}
    except Exception as e:
        logger.warning(f"Error parsing query parameters: {str(e)}")
        return {}

    #google-analytics-data==0.17.2
    #functions-framework==3.*
    #google-auth==2.28.1
    #google-cloud-aiplatform
    #vertexai
    #tiktoken==0.5.2
