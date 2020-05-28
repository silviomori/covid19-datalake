file_path = "https://github.com/silviomori/covid19-datalake/raw/" \
                "adding-task-to-spark/spark_steps/spark_step_one.py"


definition = [{
    "Name":"Spark Step One",
    "ActionOnFailure":"CONTINUE",
    "Type":"CUSTOM_JAR",
    "Jar":"command-runner.jar",
    "Args": [
        "spark-submit",
        "--deploy-mode", "client",
        "--py-files", file_path,
        file_path],
    }]


def step_one():
    print('###############################################')
    print('#############   running step one  #############')
    print('###############################################')


if __name__ == "__main__":
    step_one()

