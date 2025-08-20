"""
GA4事前集計機能モジュール
セッション・コンバージョンデータの事前集計リクエスト生成と処理を担当
"""

from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    Filter,
    FilterExpression,
    FilterExpressionList,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)

def fetch_summary_data(client, property_id, start_date, end_date, cv_events, lp_paths=None, path_filter=None, max_workers=3):
    """
    GA4事前集計データを並列取得する
    
    Args:
        client: GA4クライアント
        property_id: プロパティID
        start_date: 開始日
        end_date: 終了日
        cv_events: コンバージョンイベント（/区切り）
        path_filter: ページパスフィルター（例: "/products/"）
        lp_paths: 特定のLPパスリスト（例: ["/entry04/", "/entry05/", "/entry06/"]）
        max_workers: 並列実行数
    
    Returns:
        Dict: 集計済みデータ
    """
    logger.info("事前集計データの取得開始")
    
    # 集計リクエストを作成
    summary_requests = [
        create_site_summary_request(property_id, start_date, end_date),
        create_page_summary_request(property_id, start_date, end_date, path_filter=path_filter, limit=20),
        create_source_summary_request(property_id, start_date, end_date),
        create_device_summary_request(property_id, start_date, end_date),
        create_user_type_summary_request(property_id, start_date, end_date),
        create_cv_page_summary_request(property_id, start_date, end_date, cv_events, path_filter=path_filter, limit=20),
        create_cv_source_summary_request(property_id, start_date, end_date, cv_events),
        create_cv_device_summary_request(property_id, start_date, end_date, cv_events),
        create_cv_hour_summary_request(property_id, start_date, end_date, cv_events),
    ]

    # path_filterが指定されている場合、各パスの個別詳細も追加取得
    """ create_single_page_detail_request、以前スラックの説明のために適当につけた関数で、そのような関数は読んでいただくとわかると思うのですが実装されてないです
    path_filterが指定されている場合,指定された特定の1ページに関する、詳細なデータを取得するためのリクエストを作成するような関数を追加すればいいという感じです
    if path_filter:
        path_list = [p.strip() for p in path_filter.split(',') if p.strip()]
        for path in path_list[:10]:  # 必要に応じて20まで増やせる
            summary_requests.append(
                create_single_page_detail_request(property_id, start_date, end_date, path, cv_events)
            )
    """
    # LP集計リクエストを追加
    lp_requests = []
    if lp_paths:
        for lp_path in lp_paths:
            lp_requests.extend(create_lp_summary_request(property_id, start_date, end_date, lp_path, cv_events))
        summary_requests.extend(lp_requests)
    
    # 並列実行
    summary_responses = execute_summary_requests_parallel(client, summary_requests, max_workers)
    
    # データを処理
    site_summary = process_site_summary(summary_responses[0]) if summary_responses[0] else {}
    
    # ページ別データ処理
    page_metrics = {"page_views": "screenPageViews", "users": "totalUsers", "sessions": "sessions", 
                   "engagement_rate": "engagementRate", "avg_session_duration": "averageSessionDuration", "bounce_rate": "bounceRate"}
    page_summary = process_dimension_summary(summary_responses[1], "page", page_metrics) if summary_responses[1] else []
    
    # ソース別データ処理
    source_metrics = {"sessions": "sessions", "users": "totalUsers", "engagement_rate": "engagementRate", 
                     "avg_session_duration": "averageSessionDuration", "bounce_rate": "bounceRate"}
    source_summary = process_dimension_summary(summary_responses[2], "source", source_metrics) if summary_responses[2] else []
    
    # デバイス別データ処理
    device_metrics = {"sessions": "sessions", "users": "totalUsers", "engagement_rate": "engagementRate", 
                     "bounce_rate": "bounceRate", "avg_session_duration": "averageSessionDuration"}
    device_summary = process_dimension_summary(summary_responses[3], "device", device_metrics) if summary_responses[3] else []
    
    # ユーザータイプ別データ処理
    user_type_metrics = {"users": "totalUsers", "sessions": "sessions", "engagement_rate": "engagementRate", 
                        "avg_session_duration": "averageSessionDuration", "bounce_rate": "bounceRate"}
    user_type_summary = process_dimension_summary(summary_responses[4], "user_type", user_type_metrics) if summary_responses[4] else []
    
    # コンバージョン集計データ処理
    cv_responses = summary_responses[5:9]  # ページ、メディア、デバイス、時間帯
    conversion_summary = process_conversion_summary(cv_responses, cv_events)
    
    # LP集計データ処理
    lp_summary = {}
    if lp_paths:
        lp_responses = summary_responses[9:9+len(lp_requests)]  # LP集計レスポンス
        for i, lp_path in enumerate(lp_paths):
            start_idx = i * 2  # 各LPは2つのリクエスト（基本指標 + コンバージョン）
            if start_idx + 1 < len(lp_responses):
                lp_data = [lp_responses[start_idx], lp_responses[start_idx + 1]]
                lp_summary[lp_path] = process_lp_summary(lp_data, cv_events)
    
    logger.info("集計データの処理完了")
    
    return {
        "site": site_summary,
        "by_page": page_summary,
        "by_source": source_summary,
        "by_device": device_summary,
        "by_user_type": user_type_summary,
        "conversions": conversion_summary,
        "by_lp": lp_summary
    }

def create_site_summary_request(property_id, start_date, end_date):
    """サイト全体の集計リクエストを生成"""
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[],  # ディメンションなし = 全体集計
        metrics=[
            Metric(name="totalUsers"),
            Metric(name="newUsers"), 
            Metric(name="sessions"),
            Metric(name="screenPageViews"),
            Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"),
            Metric(name="userEngagementDuration"),
        ]
    )

def create_page_summary_request(property_id, start_date, end_date, path_filter=None, limit=20):
    """ページ別集計リクエストを生成（上位N ページ）"""
    
    # pathフィルターの設定
    dimension_filter = None
    # path_filterがNoneでない場合のみ実行するよう修正すると、より安全です。
    if path_filter:
        path_list = [p.strip() for p in path_filter.split(',') if p.strip()]
            
        if len(path_list) == 1:
            # 単一パス
            dimension_filter = FilterExpression(
                filter=Filter(
                    field_name='pagePath',
                    string_filter=Filter.StringFilter(
                        value=path_list[0],
                        match_type=Filter.StringFilter.MatchType.EXACT  # CONTAINSからEXACTに変更
                    )
                )
            )
        elif len(path_list) > 1: # 複数パスの場合を明示的にするため `else` から `elif` に変更
            # 複数パス（OR条件）
            filters = []
            for path in path_list:
                filters.append(FilterExpression(
                    filter=Filter(
                        field_name='pagePath',
                        string_filter=Filter.StringFilter(
                            value=path,
                            match_type=Filter.StringFilter.MatchType.EXACT
                        )
                    )
                ))
            dimension_filter = FilterExpression(
                or_group=FilterExpressionList(expressions=filters)
            )    
    
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePath")],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="totalUsers"),
            Metric(name="sessions"),
            Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"),
        ],
        dimension_filter=dimension_filter,
        order_bys=[{"metric": {"metric_name": "screenPageViews"}, "desc": True}],
        limit=limit
    )

def create_source_summary_request(property_id, start_date, end_date):
    """ソース別集計リクエストを生成"""
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="sessionSource")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"),
        ],
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        limit=20
    )

def create_device_summary_request(property_id, start_date, end_date):
    """デバイス別集計リクエストを生成"""
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="deviceCategory")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="engagementRate"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
        ],
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}]
    )

def create_user_type_summary_request(property_id, start_date, end_date):
    """ユーザータイプ別集計リクエストを生成"""
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="newVsReturning")],
        metrics=[
            Metric(name="totalUsers"),
            Metric(name="sessions"),
            Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"),
        ]
    )

def create_cv_page_summary_request(property_id, start_date, end_date, cv_events, path_filter=None, limit=20):
    """コンバージョンのページ別集計リクエストを生成"""
    event_names = cv_events.split('/')
    
    # イベントフィルターの構築
    if len(event_names) == 1:
        event_filter = FilterExpression(
            filter=Filter(
                field_name='eventName',
                string_filter=Filter.StringFilter(value=event_names[0], match_type=Filter.StringFilter.MatchType.EXACT)
            )
        )
    else:
        filter_expressions = []
        for event_name in event_names:
            filter_expressions.append(FilterExpression(
                filter=Filter(
                    field_name='eventName',
                    string_filter=Filter.StringFilter(value=event_name.strip(), match_type=Filter.StringFilter.MatchType.EXACT)
                )
            ))
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=filter_expressions))
    
    # pathフィルターとイベントフィルターを組み合わせる
    final_filter = event_filter
    if path_filter:
        path_filter_expr = FilterExpression(
            filter=Filter(
                field_name='pagePath',
                string_filter=Filter.StringFilter(value=path_filter, match_type=Filter.StringFilter.MatchType.CONTAINS)
            )
        )
        # AND条件で組み合わせ
        final_filter = FilterExpression(
            and_group=FilterExpressionList(expressions=[event_filter, path_filter_expr])
        )
    
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="pagePath"),
            Dimension(name="eventName"),
        ],
        metrics=[Metric(name="eventCount")],
        dimension_filter=final_filter,
        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}],
        limit=limit
    )

def create_cv_source_summary_request(property_id, start_date, end_date, cv_events):
    """コンバージョンのソース別集計リクエストを生成"""
    event_names = cv_events.split('/')
    
    # イベントフィルターの構築
    if len(event_names) == 1:
        event_filter = FilterExpression(
            filter=Filter(
                field_name='eventName',
                string_filter=Filter.StringFilter(value=event_names[0], match_type=Filter.StringFilter.MatchType.EXACT)
            )
        )
    else:
        filter_expressions = []
        for event_name in event_names:
            filter_expressions.append(FilterExpression(
                filter=Filter(
                    field_name='eventName',
                    string_filter=Filter.StringFilter(value=event_name.strip(), match_type=Filter.StringFilter.MatchType.EXACT)
                )
            ))
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=filter_expressions))
    
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="sessionSource"),
            Dimension(name="eventName"),
        ],
        metrics=[Metric(name="eventCount")],
        dimension_filter=event_filter,
        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}],
        limit=20
    )

def create_cv_device_summary_request(property_id, start_date, end_date, cv_events):
    """コンバージョンのデバイス別集計リクエストを生成"""
    event_names = cv_events.split('/')
    
    # イベントフィルターの構築
    if len(event_names) == 1:
        event_filter = FilterExpression(
            filter=Filter(
                field_name='eventName',
                string_filter=Filter.StringFilter(value=event_names[0], match_type=Filter.StringFilter.MatchType.EXACT)
            )
        )
    else:
        filter_expressions = []
        for event_name in event_names:
            filter_expressions.append(FilterExpression(
                filter=Filter(
                    field_name='eventName',
                    string_filter=Filter.StringFilter(value=event_name.strip(), match_type=Filter.StringFilter.MatchType.EXACT)
                )
            ))
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=filter_expressions))
    
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="deviceCategory"),
            Dimension(name="eventName"),
        ],
        metrics=[Metric(name="eventCount")],
        dimension_filter=event_filter,
        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}]
    )

def create_cv_hour_summary_request(property_id, start_date, end_date, cv_events):
    """コンバージョンの時間帯別集計リクエストを生成"""
    event_names = cv_events.split('/')
    
    # イベントフィルターの構築
    if len(event_names) == 1:
        event_filter = FilterExpression(
            filter=Filter(
                field_name='eventName',
                string_filter=Filter.StringFilter(value=event_names[0], match_type=Filter.StringFilter.MatchType.EXACT)
            )
        )
    else:
        filter_expressions = []
        for event_name in event_names:
            filter_expressions.append(FilterExpression(
                filter=Filter(
                    field_name='eventName',
                    string_filter=Filter.StringFilter(value=event_name.strip(), match_type=Filter.StringFilter.MatchType.EXACT)
                )
            ))
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=filter_expressions))
    
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="hour"),
            Dimension(name="eventName"),
        ],
        metrics=[Metric(name="eventCount")],
        dimension_filter=event_filter,
        order_bys=[{"dimension": {"dimension_name": "hour"}}]
    )

def create_lp_summary_request(property_id, start_date, end_date, lp_path, cv_events):
    """特定のLPパスの集計リクエストを生成（PV・セッション・CVを含む）"""
    
    # 1. LPの基本指標（PV・セッション・ユーザー数など）を取得するリクエスト
    lp_basic_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePath")],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="totalUsers"),
            Metric(name="sessions"),
            Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"),
        ],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name='pagePath',
                string_filter=Filter.StringFilter(value=lp_path, match_type=Filter.StringFilter.MatchType.EXACT)
            )
        )
    )
    
    # 2. LPのコンバージョン数を取得するリクエスト
    event_names = cv_events.split('/')
    
    # イベントフィルターの構築
    if len(event_names) == 1:
        event_filter = FilterExpression(
            filter=Filter(
                field_name='eventName',
                string_filter=Filter.StringFilter(value=event_names[0], match_type=Filter.StringFilter.MatchType.EXACT)
            )
        )
    else:
        filter_expressions = []
        for event_name in event_names:
            filter_expressions.append(FilterExpression(
                filter=Filter(
                    field_name='eventName',
                    string_filter=Filter.StringFilter(value=event_name.strip(), match_type=Filter.StringFilter.MatchType.EXACT)
                )
            ))
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=filter_expressions))
    
    # pathフィルターを追加
    path_filter_expr = FilterExpression(
        filter=Filter(
            field_name='pagePath',
            string_filter=Filter.StringFilter(value=lp_path, match_type=Filter.StringFilter.MatchType.EXACT)
        )
    )
    final_filter = FilterExpression(
        and_group=FilterExpressionList(expressions=[event_filter, path_filter_expr])
    )
    
    lp_cv_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="pagePath"),
            Dimension(name="eventName"),
        ],
        metrics=[Metric(name="eventCount")],
        dimension_filter=final_filter,
        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}],
        limit=20
    )
    
    return [lp_basic_request, lp_cv_request]

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

def process_dimension_summary(response, dimension_name, metric_mapping):
    """ディメンション別集計レスポンスを汎用的に処理"""
    result = []
    for row in response.rows:
        item = {dimension_name: row.dimension_values[0].value}
        for i, (key, _) in enumerate(metric_mapping.items()):
            value = row.metric_values[i].value if row.metric_values[i].value else "0"
            if key in ["engagement_rate", "avg_session_duration", "bounce_rate"]:
                item[key] = float(value)
            else:
                item[key] = int(value)
        result.append(item)
    return result

def process_conversion_summary(responses, cv_events):
    """コンバージョン集計レスポンスを処理"""
    cv_page_response, cv_source_response, cv_device_response, cv_hour_response = responses
    
    # イベント別総数を計算
    event_names = cv_events.split('/')
    total_by_event = {event.strip(): 0 for event in event_names}
    
    # ページ別データから総数を集計
    for row in cv_page_response.rows:
        event_name = row.dimension_values[1].value
        count = int(row.metric_values[0].value) if row.metric_values[0].value else 0
        if event_name in total_by_event:
            total_by_event[event_name] += count
    
    # ページ別集計
    by_page = []
    current_page = None
    page_data = {}
    
    for row in cv_page_response.rows:
        page_path = row.dimension_values[0].value
        event_name = row.dimension_values[1].value
        count = int(row.metric_values[0].value) if row.metric_values[0].value else 0
        
        if current_page != page_path:
            if current_page is not None:
                by_page.append(page_data)
            current_page = page_path
            page_data = {"path": page_path}
            for event in event_names:
                page_data[event.strip()] = 0
        
        if event_name in page_data:
            page_data[event_name] = count
    
    if current_page is not None:
        by_page.append(page_data)
    
    # ソース別・デバイス別・時間帯別も同様に処理
    by_source = process_cv_dimension_data(cv_source_response, "source", event_names)
    by_device = process_cv_dimension_data(cv_device_response, "device", event_names)
    by_hour = process_cv_hour_data(cv_hour_response, event_names)
    
    return {
        "total": total_by_event,
        "by_page": by_page,
        "by_source": by_source,
        "by_device": by_device,
        "by_hour": by_hour
    }

def process_lp_summary(responses, cv_events):
    """特定のLPパスの集計レスポンスを処理（基本指標とコンバージョンを含む）"""
    if not responses or len(responses) < 2:
        return {}
    
    basic_response = responses[0]
    cv_response = responses[1]
    
    # 基本指標の処理
    basic_metrics = {}
    if basic_response and basic_response.rows:
        row = basic_response.rows[0]
        basic_metrics = {
            "page_views": int(row.metric_values[0].value) if row.metric_values[0].value else 0,
            "users": int(row.metric_values[1].value) if row.metric_values[1].value else 0,
            "sessions": int(row.metric_values[2].value) if row.metric_values[2].value else 0,
            "engagement_rate": float(row.metric_values[3].value) if row.metric_values[3].value else 0.0,
            "avg_session_duration": float(row.metric_values[4].value) if row.metric_values[4].value else 0.0,
            "bounce_rate": float(row.metric_values[5].value) if row.metric_values[5].value else 0.0,
        }
    
    # コンバージョンの処理
    event_names = cv_events.split('/')
    total_by_event = {event.strip(): 0 for event in event_names}
    
    if cv_response and cv_response.rows:
        for row in cv_response.rows:
            event_name = row.dimension_values[1].value
            count = int(row.metric_values[0].value) if row.metric_values[0].value else 0
            if event_name in total_by_event:
                total_by_event[event_name] += count
    
    # コンバージョン率の計算
    conversion_rate = 0.0
    total_conversions = sum(total_by_event.values())
    if basic_metrics.get("sessions", 0) > 0:
        conversion_rate = (total_conversions / basic_metrics["sessions"]) * 100
    
    return {
        "basic_metrics": basic_metrics,
        "conversions": total_by_event,
        "total_conversions": total_conversions,
        "conversion_rate": round(conversion_rate, 2)
    }

def process_cv_dimension_data(response, dimension_key, event_names):
    """コンバージョンのディメンション別データを処理"""
    result = []
    current_dim = None
    dim_data = {}
    
    for row in response.rows:
        dim_value = row.dimension_values[0].value
        event_name = row.dimension_values[1].value
        count = int(row.metric_values[0].value) if row.metric_values[0].value else 0
        
        if current_dim != dim_value:
            if current_dim is not None:
                result.append(dim_data)
            current_dim = dim_value
            dim_data = {dimension_key: dim_value}
            for event in event_names:
                dim_data[event.strip()] = 0
        
        if event_name in dim_data:
            dim_data[event_name] = count
    
    if current_dim is not None:
        result.append(dim_data)
    
    return result

def process_cv_hour_data(response, event_names):
    """コンバージョンの時間帯別データを処理"""
    hour_data = {}
    
    # 0-23時の初期化
    for hour in range(24):
        hour_data[hour] = {event.strip(): 0 for event in event_names}
    
    for row in response.rows:
        hour = int(row.dimension_values[0].value) if row.dimension_values[0].value else 0
        event_name = row.dimension_values[1].value
        count = int(row.metric_values[0].value) if row.metric_values[0].value else 0
        
        if hour in hour_data and event_name in hour_data[hour]:
            hour_data[hour][event_name] = count
    
    # 辞書をリスト形式に変換
    result = []
    for hour in range(24):
        hour_item = {"hour": hour}
        hour_item.update(hour_data[hour])
        result.append(hour_item)
    
    return result

def execute_summary_requests_parallel(client, requests, max_workers=3):
    """集計リクエストを並列実行"""
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 各リクエストを並列実行
            future_to_index = {
                executor.submit(client.run_report, request): i 
                for i, request in enumerate(requests)
            }
            
            # 結果を元の順序で取得
            responses = [None] * len(requests)
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    response = future.result()
                    responses[index] = response
                    logger.info(f"集計リクエスト{index+1}完了: {len(response.rows)}行")
                except Exception as e:
                    logger.error(f"集計リクエスト{index+1}エラー: {str(e)}")
                    responses[index] = None
            
            return responses
    except Exception as e:
        logger.error(f"並列実行エラー: {str(e)}")
        return [None] * len(requests) 

