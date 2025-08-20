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
        path_filter: ページパスフィルター（カンマ区切りで複数指定可能、完全一致）
        lp_paths: 特定のLPパスリスト（例: ["/entry04/", "/entry05/", "/entry06/"]）
        max_workers: 並列実行数
    
    Returns:
        Dict: 集計済みデータ
    """
    logger.info("事前集計データの取得開始")
    
    # 基本の集計リクエストを作成
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
    
    page_metrics = {"page_views": "screenPageViews", "users": "totalUsers", "sessions": "sessions", 
                   "engagement_rate": "engagementRate", "avg_session_duration": "averageSessionDuration", "bounce_rate": "bounceRate"}
    page_summary = process_dimension_summary(summary_responses[1], "page", page_metrics) if summary_responses[1] else []
    
    source_metrics = {"sessions": "sessions", "users": "totalUsers", "engagement_rate": "engagementRate", 
                     "avg_session_duration": "averageSessionDuration", "bounce_rate": "bounceRate"}
    source_summary = process_dimension_summary(summary_responses[2], "source", source_metrics) if summary_responses[2] else []
    
    device_metrics = {"sessions": "sessions", "users": "totalUsers", "engagement_rate": "engagementRate", 
                     "bounce_rate": "bounceRate", "avg_session_duration": "averageSessionDuration"}
    device_summary = process_dimension_summary(summary_responses[3], "device", device_metrics) if summary_responses[3] else []
    
    user_type_metrics = {"users": "totalUsers", "sessions": "sessions", "engagement_rate": "engagementRate", 
                        "avg_session_duration": "averageSessionDuration", "bounce_rate": "bounceRate"}
    user_type_summary = process_dimension_summary(summary_responses[4], "user_type", user_type_metrics) if summary_responses[4] else []
    
    cv_responses = summary_responses[5:9]
    conversion_summary = process_conversion_summary(cv_responses, cv_events)
    
    # LP集計データ処理
    lp_summary = {}
    if lp_paths:
        # 【バグ修正】レスポンスリストの末尾からLPのレスポンスを安全に取得
        lp_responses = summary_responses[-len(lp_requests):] if lp_requests else []
        
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
        dimensions=[],
        metrics=[
            Metric(name="totalUsers"), Metric(name="newUsers"), Metric(name="sessions"),
            Metric(name="screenPageViews"), Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"), Metric(name="bounceRate"),
            Metric(name="userEngagementDuration"),
        ]
    )

def create_page_summary_request(property_id, start_date, end_date, path_filter=None, limit=20):
    """ページ別集計リクエストを生成（上位N ページ or 特定ページ）"""
    dimension_filter = None
    if path_filter:
        path_list = [p.strip() for p in path_filter.split(',') if p.strip()]
        if len(path_list) == 1:
            dimension_filter = FilterExpression(filter=Filter(field_name='pagePath', string_filter=Filter.StringFilter(value=path_list[0], match_type=Filter.StringFilter.MatchType.EXACT)))
        elif len(path_list) > 1:
            filters = [FilterExpression(filter=Filter(field_name='pagePath', string_filter=Filter.StringFilter(value=path, match_type=Filter.StringFilter.MatchType.EXACT))) for path in path_list]
            dimension_filter = FilterExpression(or_group=FilterExpressionList(expressions=filters))
    
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePath")],
        metrics=[
            Metric(name="screenPageViews"), Metric(name="totalUsers"), Metric(name="sessions"),
            Metric(name="engagementRate"), Metric(name="averageSessionDuration"), Metric(name="bounceRate"),
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
            Metric(name="sessions"), Metric(name="totalUsers"), Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"), Metric(name="bounceRate"),
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
            Metric(name="sessions"), Metric(name="totalUsers"), Metric(name="engagementRate"),
            Metric(name="bounceRate"), Metric(name="averageSessionDuration"),
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
            Metric(name="totalUsers"), Metric(name="sessions"), Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"), Metric(name="bounceRate"),
        ]
    )

def create_cv_page_summary_request(property_id, start_date, end_date, cv_events, path_filter=None, limit=20):
    """コンバージョンのページ別集計リクエストを生成"""
    event_names = [e.strip() for e in cv_events.split('/') if e.strip()]
    
    if len(event_names) == 1:
        event_filter = FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=event_names[0])))
    else:
        event_filters = [FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=name))) for name in event_names]
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=event_filters))

    # 【動作統一】path_filterのロジックを修正
    path_filter_expr = None
    if path_filter:
        path_list = [p.strip() for p in path_filter.split(',') if p.strip()]
        if len(path_list) == 1:
            path_filter_expr = FilterExpression(filter=Filter(field_name='pagePath', string_filter=Filter.StringFilter(value=path_list[0], match_type=Filter.StringFilter.MatchType.EXACT)))
        elif len(path_list) > 1:
            path_filters = [FilterExpression(filter=Filter(field_name='pagePath', string_filter=Filter.StringFilter(value=path, match_type=Filter.StringFilter.MatchType.EXACT))) for path in path_list]
            path_filter_expr = FilterExpression(or_group=FilterExpressionList(expressions=path_filters))

    final_filter = event_filter
    if path_filter_expr:
        final_filter = FilterExpression(and_group=FilterExpressionList(expressions=[event_filter, path_filter_expr]))
    
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePath"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        dimension_filter=final_filter,
        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}],
        limit=limit
    )

def create_cv_source_summary_request(property_id, start_date, end_date, cv_events):
    """コンバージョンのソース別集計リクエストを生成"""
    event_names = [e.strip() for e in cv_events.split('/') if e.strip()]
    if len(event_names) == 1:
        event_filter = FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=event_names[0])))
    else:
        event_filters = [FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=name))) for name in event_names]
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=event_filters))
    
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="sessionSource"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        dimension_filter=event_filter,
        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}],
        limit=20
    )

def create_cv_device_summary_request(property_id, start_date, end_date, cv_events):
    """コンバージョンのデバイス別集計リクエストを生成"""
    event_names = [e.strip() for e in cv_events.split('/') if e.strip()]
    if len(event_names) == 1:
        event_filter = FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=event_names[0])))
    else:
        event_filters = [FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=name))) for name in event_names]
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=event_filters))
        
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="deviceCategory"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        dimension_filter=event_filter,
        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}]
    )

def create_cv_hour_summary_request(property_id, start_date, end_date, cv_events):
    """コンバージョンの時間帯別集計リクエストを生成"""
    event_names = [e.strip() for e in cv_events.split('/') if e.strip()]
    if len(event_names) == 1:
        event_filter = FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=event_names[0])))
    else:
        event_filters = [FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=name))) for name in event_names]
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=event_filters))

    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="hour"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        dimension_filter=event_filter,
        order_bys=[{"dimension": {"dimension_name": "hour"}}]
    )

def create_lp_summary_request(property_id, start_date, end_date, lp_path, cv_events):
    """特定のLPパスの集計リクエストを生成（基本指標 + コンバージョン）"""
    # 1. 基本指標リクエスト
    lp_basic_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePath")],
        metrics=[
            Metric(name="screenPageViews"), Metric(name="totalUsers"), Metric(name="sessions"),
            Metric(name="engagementRate"), Metric(name="averageSessionDuration"), Metric(name="bounceRate"),
        ],
        dimension_filter=FilterExpression(filter=Filter(field_name='pagePath', string_filter=Filter.StringFilter(value=lp_path, match_type=Filter.StringFilter.MatchType.EXACT)))
    )
    
    # 2. コンバージョンリクエスト
    event_names = [e.strip() for e in cv_events.split('/') if e.strip()]
    if len(event_names) == 1:
        event_filter = FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=event_names[0])))
    else:
        event_filters = [FilterExpression(filter=Filter(field_name='eventName', string_filter=Filter.StringFilter(value=name))) for name in event_names]
        event_filter = FilterExpression(or_group=FilterExpressionList(expressions=event_filters))
    
    path_filter_expr = FilterExpression(filter=Filter(field_name='pagePath', string_filter=Filter.StringFilter(value=lp_path, match_type=Filter.StringFilter.MatchType.EXACT)))
    final_filter = FilterExpression(and_group=FilterExpressionList(expressions=[event_filter, path_filter_expr]))
    
    lp_cv_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePath"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        dimension_filter=final_filter,
        limit=20
    )
    
    return [lp_basic_request, lp_cv_request]

def process_site_summary(response):
    """サイト全体集計レスポンスを処理"""
    if not response or not response.rows:
        return {"total_users": 0, "new_users": 0, "sessions": 0, "page_views": 0, "engagement_rate": 0.0, "avg_session_duration": 0.0, "bounce_rate": 0.0, "avg_engagement_duration": 0.0}
    
    row = response.rows[0]
    values = [val.value for val in row.metric_values]
    
    sessions = int(values[2]) if values[2] else 0
    total_engagement_duration = float(values[7]) if values[7] else 0.0
    avg_engagement_duration = (total_engagement_duration / sessions) if sessions > 0 else 0.0
    
    return {
        "total_users": int(values[0]) if values[0] else 0,
        "new_users": int(values[1]) if values[1] else 0,
        "sessions": sessions,
        "page_views": int(values[3]) if values[3] else 0,
        "engagement_rate": float(values[4]) if values[4] else 0.0,
        "avg_session_duration": float(values[5]) if values[5] else 0.0,
        "bounce_rate": float(values[6]) if values[6] else 0.0,
        "avg_engagement_duration": round(avg_engagement_duration, 2),
    }

def process_dimension_summary(response, dimension_name, metric_mapping):
    """ディメンション別集計レスポンスを汎用的に処理"""
    if not response: return []
    result = []
    for row in response.rows:
        item = {dimension_name: row.dimension_values[0].value}
        for i, (key, _) in enumerate(metric_mapping.items()):
            value_str = row.metric_values[i].value if row.metric_values[i].value else "0"
            try:
                item[key] = float(value_str) if '.' in value_str or 'e' in value_str.lower() else int(value_str)
            except (ValueError, TypeError):
                item[key] = 0
        result.append(item)
    return result

def process_conversion_summary(responses, cv_events):
    """コンバージョン集計レスポンスを処理"""
    if not all(responses):
        return {"total": {}, "by_page": [], "by_source": [], "by_device": [], "by_hour": []}
        
    cv_page_response, cv_source_response, cv_device_response, cv_hour_response = responses
    event_names = [e.strip() for e in cv_events.split('/') if e.strip()]
    total_by_event = {event: 0 for event in event_names}
    
    if cv_page_response:
        for row in cv_page_response.rows:
            event_name = row.dimension_values[1].value
            if event_name in total_by_event:
                total_by_event[event_name] += int(row.metric_values[0].value or 0)
    
    return {
        "total": total_by_event,
        "by_page": process_cv_dimension_data(cv_page_response, "path", event_names),
        "by_source": process_cv_dimension_data(cv_source_response, "source", event_names),
        "by_device": process_cv_dimension_data(cv_device_response, "device", event_names),
        "by_hour": process_cv_hour_data(cv_hour_response, event_names)
    }

def process_lp_summary(responses, cv_events):
    """特定のLPパスの集計レスポンスを処理"""
    if not responses or len(responses) < 2 or not all(responses): return {}
    
    basic_response, cv_response = responses
    basic_metrics = {}
    if basic_response.rows:
        row = basic_response.rows[0]
        values = [val.value for val in row.metric_values]
        basic_metrics = {
            "page_views": int(values[0] or 0), "users": int(values[1] or 0),
            "sessions": int(values[2] or 0), "engagement_rate": float(values[3] or 0.0),
            "avg_session_duration": float(values[4] or 0.0), "bounce_rate": float(values[5] or 0.0),
        }
    
    event_names = [e.strip() for e in cv_events.split('/') if e.strip()]
    total_by_event = {event: 0 for event in event_names}
    if cv_response.rows:
        for row in cv_response.rows:
            event_name = row.dimension_values[1].value
            if event_name in total_by_event:
                total_by_event[event_name] += int(row.metric_values[0].value or 0)
    
    total_conversions = sum(total_by_event.values())
    sessions = basic_metrics.get("sessions", 0)
    conversion_rate = (total_conversions / sessions * 100) if sessions > 0 else 0.0
    
    return {
        "basic_metrics": basic_metrics,
        "conversions": total_by_event,
        "total_conversions": total_conversions,
        "conversion_rate": round(conversion_rate, 2)
    }

def process_cv_dimension_data(response, dimension_key, event_names):
    """コンバージョンのディメンション別データを汎用的に処理"""
    if not response: return []
    result_map = {}
    for row in response.rows:
        dim_value = row.dimension_values[0].value
        event_name = row.dimension_values[1].value
        count = int(row.metric_values[0].value or 0)
        
        if dim_value not in result_map:
            result_map[dim_value] = {dimension_key: dim_value, **{event: 0 for event in event_names}}
        if event_name in result_map[dim_value]:
            result_map[dim_value][event_name] = count
    return list(result_map.values())

def process_cv_hour_data(response, event_names):
    """コンバージョンの時間帯別データを処理"""
    if not response: return []
    hour_data = {str(hour): {event: 0 for event in event_names} for hour in range(24)}
    for row in response.rows:
        hour, event_name = row.dimension_values[0].value, row.dimension_values[1].value
        count = int(row.metric_values[0].value or 0)
        if hour in hour_data and event_name in hour_data[hour]:
            hour_data[hour][event_name] = count
    
    result = []
    for hour_str, events in sorted(hour_data.items(), key=lambda item: int(item[0])):
        result.append({"hour": int(hour_str), **events})
    return result

def execute_summary_requests_parallel(client, requests, max_workers=3):
    """集計リクエストを並列実行"""
    if not requests: return []
    responses = [None] * len(requests)
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {executor.submit(client.run_report, request=req): i for i, req in enumerate(requests)}
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    responses[index] = future.result()
                except Exception as e:
                    logger.error(f"集計リクエスト{index+1}エラー: {str(e)}")
    except Exception as e:
        logger.error(f"並列実行エラー: {str(e)}")
    return responses
