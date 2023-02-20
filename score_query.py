import asyncio
from typing import List, Awaitable

from google.protobuf.json_format import MessageToDict
from httpx import AsyncClient

from _321CQU.sql_helper import DatabaseConfig
from _321CQU.tools.gRPCManager import gRPCManager
from _321CQU.service import ServiceEnum
from _321CQU.tools.httpServiceManager import HttpServiceManager

from micro_services_protobuf.model.score import Score
from micro_services_protobuf.model.cqu_session import CQUSession
import micro_services_protobuf.notification_center.apns_pb2 as apns_model
import micro_services_protobuf.notification_center.service_pb2_grpc as notification_grpc
import micro_services_protobuf.edu_admin_center.eac_service_pb2_grpc as eac_grpc
import micro_services_protobuf.edu_admin_center.eac_models_pb2 as eac_model
import micro_services_protobuf.mycqu_service.mycqu_request_response_pb2 as mycqu_model

from utils.SqlManager import SqlManager
from utils.ConfigReader import ConfigReader

__all__ = ['get_new_scores', 'send_notification']


async def get_new_scores(uid: bytes, curr_term: CQUSession) -> List[Score]:
    async with SqlManager().cursor(DatabaseConfig.User) as cursor:
        await cursor.execute('select auth, sid, password from UserAuthBind where uid = %s', (uid,))
        auth, sid, password = await cursor.fetchone()

    async with SqlManager().cursor(DatabaseConfig.Score) as cursor:
        await cursor.execute('select cid from Score where uid = %s and term = %s union '
                             'select cid from ScoreCache where uid = %s and term = %s',
                             (uid, str(curr_term), uid, str(curr_term)))
        curr_cids = list(map(lambda x: x[0], await cursor.fetchall()))

    async with gRPCManager().get_stub(ServiceEnum.EduAdminCenter) as stub:
        stub: eac_grpc.EduAdminCenterStub
        scores: mycqu_model.FetchScoreResponse = await stub.FetchScore(
            eac_model.FetchScoreRequest(
                base_login_info=mycqu_model.BaseLoginInfo(auth=auth, password=password),
                sid=sid,
                is_minor=False
            ))

    new_score = filter(lambda x: x.course.code not in curr_cids and x.session.year == curr_term.year and x.session.is_autumn == curr_term.is_autumn,
                       scores.scores)

    return [Score.parse_obj(MessageToDict(item, including_default_value_fields=True, preserving_proto_field_name=True))
            for item in new_score]


async def send_notification(uid: bytes, scores: List[Score]):
    if len(scores) == 0:
        return
    async with SqlManager().cursor(DatabaseConfig.Notification) as cursor:
        await cursor.execute('select apn from Apns where uid = %s', (uid,))
        apn_res = await cursor.fetchone()
        await cursor.execute('select openid from Openid where uid = %s', (uid,))
        openid_res = await cursor.fetchone()

    task: List[Awaitable] = []
    if len(apn_res) != 0:
        task.append(send_ios_notification(apn_res[0], scores))
    if len(openid_res) != 0:
        task.append(send_wechat_notification(openid_res[0], scores))

    await asyncio.gather(*task)


async def send_ios_notification(apn: bytes, scores: List[Score]):
    async with gRPCManager().get_stub(ServiceEnum.ApnsService) as stub:
        stub: notification_grpc.ApnsStub
        await stub.SendNotificationToUser(
            apns_model.SendApnsNotificationRequest(
                apn=apn,
                notification=apns_model.SendApnsNotificationRequest.AppleNotification(
                    alert=apns_model.SendApnsNotificationRequest.AppleNotification.AppleAlert(
                        title='成绩通知',
                        body=''.join([f"{score.course.name}: {score.score}\n" for score in scores])[:-1]
                    )
                )
            ))


async def send_wechat_notification(openid: str, scores: List[Score]):
    async with AsyncClient(timeout=10) as client:
        async with asyncio.TaskGroup() as tg:
            for score in scores:
                tg.create_task(
                    client.post(
                        HttpServiceManager().host(ServiceEnum.WechatManager) + f"/notification/{openid}",
                        params={'token': ConfigReader().get_config('WechatMiniAppSetting', 'secret')},
                        json={
                            'template_id': ConfigReader().get_config('WechatMiniAppSetting', 'score_template'),
                            'data': {'thing1': {'value': score.course.name}, 'thing2': {'value': score.score}}
                        }
                    )
                )
