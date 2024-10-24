from etl_homework import flows



def test_helloworld(prefect_test_server):
    assert flows.hello_world() == 0
