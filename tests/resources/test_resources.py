from datetime import datetime, timedelta

from datastore_api.api.data.models import (
    InputFixedQuery,
    InputTimePeriodQuery,
    InputTimeQuery,
)

valid_jwt_payload = {
    "aud": "datastore",
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "sub": "testuser",
}

jwt_payload_missing_user_id = {
    "aud": "datastore",
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
}

jwt_payload_wrong_audience = {
    "aud": "wrong",
    "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
    "sub": "testuser",
}

jwt_payload_expired = {
    "aud": "datastore",
    "exp": (datetime.now() - timedelta(hours=1)).timestamp(),
    "sub": "testuser",
}

PERSON_INCOME_ALL = (
    ";unit_id;value\n"
    "0;11111111864482;21529182\n"
    "1;11111112296273;12687840\n"
    "2;11111113785911;16354872\n"
    "3;11111113735577;12982099\n"
    "4;11111111454434;19330053\n"
    "5;11111111190644;11331198\n"
    "6;11111113331169;4166169\n"
    "7;11111111923572;7394257\n"
    "8;11111112261125;6926636\n"
)

TEST_STUDIEPOENG_ALL = (
    ";unit_id;value\n"
    "0;11111111111123;1000\n"
    "1;11111111111124;1000\n"
    "2;11111111111125;1000\n"
    "3;11111111111123;2000\n"
    "4;11111111111124;2000\n"
    "5;11111111111125;2000\n"
    "6;11111111111123;3000\n"
    "7;11111111111124;3000\n"
    "8;11111111111125;3000\n"
)

PERSON_INCOME_LAST_ROW = ";unit_id;value\n0;11111112261125;6926636\n"

VALID_EVENT_QUERY_PERSON_INCOME_ALL = InputTimePeriodQuery(
    dataStructureName="TEST_PERSON_INCOME",
    version="1.0.0.0",
    startDate=1,
    stopDate=32767,
)

VALID_EVENT_QUERY_TEST_STUDIEPOENG_ALL = InputTimePeriodQuery(
    dataStructureName="TEST_STUDIEPOENG",
    version="1.0.0.0",
    startDate=1,
    stopDate=32767,
)

INVALID_EVENT_QUERY_INVALID_STOP_DATE = InputTimePeriodQuery(
    dataStructureName="TEST_PERSON_INCOME",
    version="1.0.0.0",
    startDate=100,
    stopDate=99,
)

VALID_STATUS_QUERY_PERSON_INCOME_LAST_ROW = InputTimeQuery(
    dataStructureName="TEST_PERSON_INCOME", version="1.0.0.0", date=12200
)

INVALID_STATUS_QUERY_NOT_FOUND = InputTimeQuery(
    dataStructureName="NOT_A_DATASET", version="1.0.0.0", date=123
)

VALID_FIXED_QUERY_PERSON_INCOME_ALL = InputFixedQuery(
    dataStructureName="TEST_PERSON_INCOME", version="1.0.0.0"
)

INVALID_FIXED_QUERY_NOT_FOUND = InputFixedQuery(
    dataStructureName="NOT_A_DATASET", version="1.0.0.0"
)
