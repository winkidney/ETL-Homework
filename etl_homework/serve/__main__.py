from etl_homework import flows


if __name__ == "__main__":
    flows.hello_world.serve(
        name="my-first-deployment",
        tags=["onboarding"],
        parameters={"goodbye": True},
        cron="* * * * *",
    )
