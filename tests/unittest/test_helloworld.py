from etl_homework import flows


def test_hello_world(prefect_unittest_conf):
    assert flows.hello_world() == 0
