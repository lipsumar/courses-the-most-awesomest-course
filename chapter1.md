---
title: 'Testing your data pipeline'
description: 'Stating “it works on my machine” is not a guarantee it will work reliably elsewhere and in the future. Requirements for your project will change. In this chapter, we explore different forms of testing and learn how to write unit tests for our PySpark data transformation pipeline, so that we make robust and reusable parts.'
attachments:
    slides_link: 'https://s3.amazonaws.com/assets.datacamp.com/production/course_18306/slides/chapter3.pdf'
---

## On the importance of tests

```yaml
type: VideoExercise
key: f857940310
xp: 50
```

`@projector_key`
e2d7d8e6798da7d9d583d9ffa4d6e6c1

---

## Regression errors

```yaml
type: PureMultipleChoiceExercise
key: a1591ddacc
xp: 50
```

A common software versioning system is called “semantic versioning” or “semver”!!!!!. In it, three numbers, separated by dots, specify the version. The first number is increased whenever there are major changes, like between Python 2.x.y and Python 3.u.v. Whenever there are minor changes, like newly added features, the second number is increased, e.g. 3.1.2 and 3.2.0. The last number is used when bugfixes are made on a minor release. The changes are too small to require lots of documentation then.

A piece of software from your organization is released, as version 2.3.1. One of the users of this software quickly files a complaint stating that the software no longer gives him the correct output for a routine task, compared to the software’s previous version, 2.3.0.

What conclusions can you draw?

`@hint`
- This is a bugfix release. No new features should've been added, no re-training should have been given either. Good features should've continued working.
- How do you ensure desired behavior remains present over time in software?

`@possible_answers`
- The end-user doesn't know how to properly install software.
- The developers of the software don’t know what they’re doing.
- The complaint submission system that the end-user used to report this regression works very well.
- [Not enough tests were in place to ensure the behavior is stable over multiple releases.]
- You can't draw any conclusions.

`@feedback`
- While that may be true, you can't say for sure.
- It's highly unlikely that people who can release software are working without purpose.
- Maybe, but you can't say for sure. Maybe this part of the ticketing system works well, but the report generation service is broken.
- Exactly! One of the many reasons to have tests is to prevent such regressions.
- Yes, you can. Such things should not happen in a production setting.

---

## Characteristics of tests

```yaml
type: DragAndDropExercise
key: d7bcad22d6
kind: Classify
xp: 100
```

Drag the cards with characteristics on them to the correct kind of test.

`@instructions`
Drag and drop cards into the correct bucket.

`@hint`
- Keep the test pyramid in mind:

![The test pyramid](https://assets.datacamp.com/production/repositories/4724/datasets/c99d9e5d420ec01ba2de798b3ec2346138da5266/test_pyramid_layer5.png)
&copy; Martin Fowler “TestPyramid”

- End-to-end tests often (but not always) go through the user interface (remark: parts of a user interface can be tested without touching many systems).

`@solution`
```{python}
- id: a
  title: Tasks
  maxOffset: 0
- id: b
  title: Unit Test
  maxOffset: 0
  items:
    - content: 'You typically have most of these.'
      id: id_0
    - content: 'They typically run in milliseconds.'
      id: id_1
    - content: 'You should be able to run these on your local laptop without configuration, aside from installing a test framework.'
      id: id_2
    - content: 'Can typically be written in a few minutes.'
      id: id_3
    - content: 'Lowercasing a column in a table is an example of this.'
      id: id_4
- id: c
  title: Service Test
  maxOffset: 0
  items:
    - content: 'Tests the interaction between a few services.'
      id: id_5
    - content: 'A database writing out to a file is an example.'
      id: id_6
    - content: 'Requires a bit of setup to have each service configured in the desired state.'
      id: id_7
- id: d
  title: End-to-end Test
  maxOffset: 0
  items:
    - content: 'Combine the services of many systems.' 
      id: id_8
    - content: 'Most difficult to debug.'
      id: id_9
    - content: 'Is closest to what an end-user experiences.'
      id: id_10
    - content: 'Subscribing for a newsletter and receiving e-mail confirmation from this is an example.'
      id: id_11
```

`@sct`
```{python}
checks:
  - condition: check_target(id_0) == solution
    incorrectMessage: 'No. Lots of small tests can cover a lot of different cases early on, so this card belongs elsewhere.'
  - condition: check_target(id_1) == solution
    incorrectMessage: 'No, have a look again at the test pyramid in the slides.'
  - condition: check_target(id_2) == solution
    incorrectMessage: 'Perhaps, but it is not as likely as with another class of tests.'
  - condition: check_target(id_3) == solution
    incorrectMessage: 'No, this kind would require more setup and thus take longer.'
  - condition: check_target(id_4) == solution
    incorrectMessage: 'No, this operation can be done within one tool or service, like a database. There is no need for integration. Inputs and expected outputs can be verified directly.'
  - condition: check_target(id_5) == solution
    incorrectMessage: 'No, unit tests only test units (single “things”), while broadstack tests cover many different systems.'
  - condition: check_target(id_6) == solution
    incorrectMessage: 'No, this example combines two distinct services. If you do not have access to write to the filesystem, your test will fail, so you have a bit of setup work to do as well.'
  - condition: check_target(id_7) == solution
    incorrectMessage: 'No, this card references multiple services and a bit of work. Not tons of work, nor the work of a single service.'
  - condition: check_target(id_8) == solution
    incorrectMessage: 'No, when you talk about many systems, you are likely covering a broad stack of services and functionality.'
  - condition: check_target(id_9) == solution
    incorrectMessage: 'No, unit tests and integration tests are typically easier to debug, because you know exactly which parts are being tested and which can fail.'
  - condition: check_target(id_10) == solution
    incorrectMessage: 'No, an end-user experiences your entire app, both frontend services, like the user interface, as well as back-end like database retrieval.'
  - condition: check_target(id_11) == solution
    incorrectMessage: 'No. Think about it: what kind of services would be hit when you click subscribe? Validation on the front- and back-end, a server that should be allowed to send automated e-mails, …'
successMessage: 'Well done! You understand the characteristics of 3 types of common tests. Be aware that there are more types of tests than just these three.'
isOrdered: false
```

---

## Writing unit tests for PySpark

```yaml
type: VideoExercise
key: 85c85df5b0
xp: 50
```

`@projector_key`
e822d78558000bac2bb64f2a10c885a3

---

## Creating in-memory DataFrames

```yaml
type: SingleProcessExercise
key: 46e985b311
xp: 100
```

Creating small datasets for unit tests is an important skill. It improves readability and understanding, because any developer looking at your code, can immediately see the inputs to some function and how they relate to the output. Additionally, you can illustrate how the function behaves with normal data and with exceptional data, like missing or incorrect fields.

`@instructions`
- Transform `data` from a tuple of tuples into a tuple of `record`s.
- Create a Spark DataFrame from this `data`.

`@hint`
- `record` is technically a class, even if it isn't _CamelCased_ like you might expect if you’re familiar with the Python style guide . That’s because the Spark docs usually show it this way and we maintain consistency with their documentation. You can instantiate it like you would any other class though, e.g. `ClassReference(initialization_parameters)`.

`@pre_exercise_code`
```{python}
_init_spark = '/home/repl/.init-spark-hidden.py' 
with open(_init_spark) as f:
	code = compile(f.read(), _init_spark, 'exec')
	exec(code)

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

`@sample_code`
```{python}
from datetime import date
from pyspark.sql import Row

record = Row("country", "utm_campaign", "airtime_in_minutes", "start_date", "end_date")

# Create a tuple of records
data = (
  ____("USA", "DiapersFirst", 28, date(2017, 1, 20), date(2017, 1, 27)),
  ____("Germany", "WindelKind", 31, date(2017, 1, 25), None),
  ____("India", "CloseToCloth", 32, date(2017, 1, 25), date(2017, 2, 2))
)

# Create a DataFrame from these records
frame = spark.____(____)
frame.show()
```

`@solution`
```{python}
from datetime import date
from pyspark.sql import Row

record = Row("country", "utm_campaign", "airtime_in_minutes", "start_date", "end_date")

# Create a tuple of records
data = (
  record("USA", "DiapersFirst", 28, date(2017, 1, 20), date(2017, 1, 27)),
  record("Germany", "WindelKind", 31, date(2017, 1, 25), None),
  record("India", "CloseToCloth", 32, date(2017, 1, 25), date(2017, 2, 2))
)

# Create a DataFrame from these records
frame = spark.createDataFrame(data)
frame.show()
```

`@sct`
```{python}
_init_spark = '/home/repl/.init-spark-hidden.py' 
with open(_init_spark) as f:
	code = compile(f.read(), _init_spark, 'exec')
	exec(code)

import datetime

Ex().check_object("data")    

Ex().multi(
	has_equal_value(override="USA", expr_code="data[0].country", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override="DiapersFirst", expr_code="data[0].utm_campaign", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override=28, expr_code="data[0].airtime_in_minutes", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override=datetime.date(2017, 1, 20), expr_code="data[0].start_date", incorrect_msg="Do not modify the values provided for each row."),
    has_equal_value(override=datetime.date(2017, 1, 27), expr_code="data[0].end_date", incorrect_msg="Do not modify the values provided for each row."),
	has_equal_value(override="Germany", expr_code="data[1].country", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override="WindelKind", expr_code="data[1].utm_campaign", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override=31, expr_code="data[1].airtime_in_minutes", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override=datetime.date(2017, 1, 25), expr_code="data[1].start_date", incorrect_msg="Do not modify the values provided for each row."),
    has_equal_value(override=None, expr_code="data[1].end_date", incorrect_msg="Do not modify the values provided for each row."),
	has_equal_value(override="India", expr_code="data[2].country", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override="CloseToCloth", expr_code="data[2].utm_campaign", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override=32, expr_code="data[2].airtime_in_minutes", incorrect_msg="Do not modify the values provided for each row."),
  	has_equal_value(override=datetime.date(2017, 1, 25), expr_code="data[2].start_date", incorrect_msg="Do not modify the values provided for each row."),
    has_equal_value(override=datetime.date(2017, 2, 2), expr_code="data[2].end_date", incorrect_msg="Do not modify the values provided for each row.")
)

Ex().check_function("spark.createDataFrame").check_args(0).has_equal_value()

success_msg("Excellent job. Get into the habit of creating small samples like this to facilitate the thinking process.")
```

---

## Making a function more widely reusable

```yaml
type: IDEExercise
key: 5c5eeb3312
xp: 100
```

With the best intentions, one of your team mates chopped up a small Spark transformation pipeline, _chinese&lowbar;provinces.py_, into smaller functions that can be used in different locations, _chinese&lowbar;provinces&lowbar;improved.py_. That’s great! Your team mate even wrote a test for one of those new functions. However, the test fails. You could verify this by running `pipenv run pytest .` when you are in the _chinese\_demographics_ folder, if you wanted, but that’s not the point of this exercise. You’ll learn more about `pytest` in the next lesson anyway.

In this exercise, you need to focus only on the _~/workspace/spark\_pipelines/chinese\_demographics_ folder.

`@instructions`
- Edit _chinese&lowbar;provinces&lowbar;improved.py_ so that the unit test would pass. To do so, read the unit test, which you’ll find in _test&lowbar;improvements.py_. There’s a large multi-line comment there that gives a good hint about what you would need to alter.

`@hint`
- You need to add just one word and a comma to one line.
- Don’t think about the code for a moment. Think about the aggregation that’s being performed: if in the city of Amsterdam, in New York, USA there’s one man with duct tape, and in Amsterdam in the Netherlands, there’s also a man with duct tape, are there then in Amsterdam really two men with duct tape? Technically, yes, but it’s not the intent to group unrelated “objects”.

`@pre_exercise_code`
```{python}
import os
import shutil

from pathlib import Path
import stat

workspace = Path.home() / "workspace" 
## Put older project folders back in place
# From ex1.12
pkg_dir = workspace / "ingest"
sol_dir = Path("/tmp/dependencies/exercises_chapter1/ingest")
shutil.rmtree(pkg_dir, ignore_errors=True)
shutil.copytree(sol_dir, pkg_dir)

# From 1.13
pkg_dir = workspace / "spark_pipelines"
sol_dir = Path("/tmp/dependencies/exercises_chapter2/spark_pipelines/")
shutil.rmtree(pkg_dir, ignore_errors=True)
shutil.copytree(sol_dir, pkg_dir)

## Put new folder of this exercise in place.
sol_dir = Path("/tmp/dependencies/exercises_chapter3/refactoring_etl")
shutil.rmtree(workspace / "spark_pipelines" / "chinese_demographics", ignore_errors=True)
shutil.copytree(sol_dir, workspace / "spark_pipelines" / "chinese_demographics")

# @SCT writers: I'd love to enable this, but for some reason, when I do, the files disappear. Anyway, it's not super important, but it could help keeping the students focused on just the one file they need to edit.
# Answer from Tech: use the YAML files.
# Reply: that's really inconvenient when you have tons of files that might still change over the development of the course. With the current course, all folders are in the git repo and are deployable.
ENABLE_READ_ONLY_FILES = False
if ENABLE_READ_ONLY_FILES:
  for file_or_dir in dir.iterdir():
    print(file_or_dir)
    if file_or_dir != dir / "chinese_provinces_improved.py":
      os.chmod(file_or_dir, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
  

```

`@sample_code`
```{python}
- path: spark_pipelines/chinese_demographics/chinese_provinces_improved.py
  content: |
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lower, sum

    from .catalog import catalog


    def extract_demographics(sparksession, catalog):
        return sparksession.read.parquet(catalog["clean/demographics"])


    def store_chinese_demographics(frame, catalog):
        frame.write.parquet(catalog["business/chinese_demographics"])


    def aggregate_inhabitants_by_province(frame):
        return (frame
                .groupBy("province")
                .agg(sum(col("inhabitants")).alias("inhabitants"))
                )


    def main():
        spark = SparkSession.builder.getOrCreate()
        frame = extract_demographics(spark, catalog)
        chinese_demographics = frame.filter(lower(col("country")) == "china")
        aggregated_demographics = aggregate_inhabitants_by_province(chinese_demographics)
        store_chinese_demographics(aggregated_demographics, catalog)


    if __name__ == "__main__":
        main()

```

`@solution`
```{python}
- permission: 755
  open: false
  path: spark_pipelines/chinese_demographics/chinese_provinces_improved.py
  content: |+
    from project.hello_world import greet
    greet()

    print("Hello World!")


```

`@sct`
```{python}
# students have to change the line       
# .groupBy("province")
# into
# .groupBy("country", "province")

#OLIVER
# On the chapter1 branch, many Spark exercises are failing and giving out an error.
# In an IDE exercise, if you do spark -shell, it fails. This should not happen. 
# test_improvements.py shoudl be read only. However, if we make it read-only (without using YAML) then the file disappears (it does not show in the editor).

# For the SCT, use a check_correct() to see if the unit test is passing. If not, open the appropriate file and check the function. Declare a test Spark DataFrame in the SCT.

#@sct writer: while I couldn't run pytest (because I'm working with virtual environments using Pipenv and that does not seem to be well supported on DataCamp), I'm sure with a few tweaks you could. If the unit test passes (exit code 0), the student has probably done a good job. However, running that SCT takes ~10s, because you have to start Spark. Maybe just check if the parameters to groupBy were correctly altered using PythonWhat and only that change was made?

#GATHER Need to define a test spark dataframe and then check the groupby and, potentially, one of the columns

# _init_spark = '/home/repl/.init-spark.py'
# with open(_init_spark) as f:
#     code = compile(f.read(), _init_spark, 'exec')
#     exec(code)


setup_solution_code = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, sum

import os
from pathlib import Path

DATA_LAKE = Path(os.environ.get("DATA_LAKE", "/mnt/data_lake"))

def _resource(zone, key):
    return str(DATA_LAKE / zone / key)

catalog = {
    "clean/demographics": _resource("clean", "demographics"),
    "business/chinese_demographics": _resource("business", "chinese_demographics")
}

def extract_demographics(sparksession, catalog):
    return sparksession.read.parquet(catalog["clean/demographics"])


def store_chinese_demographics(frame, catalog):
    frame.write.parquet(catalog["business/chinese_demographics"])


def aggregate_inhabitants_by_province(frame):
    return (frame
            .groupBy("country", "province")
            .agg(sum(col("inhabitants")).alias("inhabitants"))
            )


def main():
    spark = SparkSession.builder.getOrCreate()
    frame = extract_demographics(spark, catalog)
    chinese_demographics = frame.filter(lower(col("country")) == "china")
    aggregated_demographics = aggregate_inhabitants_by_province(chinese_demographics)
    store_chinese_demographics(aggregated_demographics, catalog)


if __name__ == "__main__":
    main()

"""

# _init_spark = '/home/repl/.init-spark-hidden.py' 
# with open(_init_spark) as f:
# 	code = compile(f.read(), _init_spark, 'exec')
# 	exec(code)

sig_group = sig_from_obj("frame.groupBy")
sig_agg = sig_from_obj("frame.groupBy.agg")
sig_alias = sig_from_obj("sum.alias")

Ex().check_file("spark_pipelines/chinese_demographics/chinese_provinces_improved.py", solution_code=setup_solution_code).run().allow_errors() \
  	.check_function_def("aggregate_inhabitants_by_province").check_body().multi(
        check_function("frame.groupBy", signature=sig_group).multi(
            check_args(0).has_equal_value(incorrect_msg="Expecting a different value for the first parameter of the `groupBy()` call in the function `aggregate_inhabitants_by_province()`."),
            check_args(1).has_equal_value()
        ),
        check_function("frame.groupBy.agg", signature=sig_agg).check_args(0).multi(
          check_function("sum")
          .check_args(0)
          .check_function("col")
          .check_args(0)
          .has_equal_value(),
          check_function("sum.alias", signature=sig_alias).check_args(0).has_equal_value()
        )
    )

success_msg("Well done! If you wanted to, you could re-use the function `aggregate_inhabitants_by_province` in other modules. Though to be fair, it’s a bit too specific for this use case as it makes a numer of assumptions about the column names.")
```

---

## Continuous testing

```yaml
type: VideoExercise
key: 295cd2a320
xp: 50
```

`@projector_key`
d94c9173e9f10d30b8abeceeecf9eca9

---

## A high-level view on CI/CD

```yaml
type: DragAndDropExercise
key: 0b24c35321
kind: Order
xp: 100
```

You have made your manager aware of your progress with the data pipeline. Now he/she wants you to prepare the code for production. To do so, you have created tests for various parts of your pipeline, so that if someone else changes the code, by simply running the unit tests it will immediately become clear if the newly introduced changes break the expected behavior. 

Now you need to help configuring the CI/CD tool that is being used in your company. You need to instruct it to retrieve the code from your company’s internal version control system and run the `pytest` module automatically.

`@instructions`
- Put the tasks in the correct order.

`@hint`
- Some tasks cannot begin before any others. E.g. you can’t make an omelet without breaking an egg. Likewise, you can’t run code if there is no code to begin with. Additionally, code might need extra tools to run in an environment.
- To install package dependencies, you should be looking for a _requirements.txt_ file (or a _Pipfile_), which is in the repo. So you should download it before installing the dependencies.
- Should you create artifacts (Python wheels, eggs, zip files, …) of something that isn’t working? How do you get an idea if the code works?
- Can you run tests without having all required dependencies installed?

`@solution`
```{python}
- id: a
  title: Tasks
  maxOffset: 0
  items: 
    - content: Check out your application from version control.
      id: id_0
    - content: Install your Python application’s dependencies.
      id: id_1
    - content: Run the test suite of your application.
      id: id_2
    - content: Create artifacts (jar, egg, wheel, documentation, …).
      id: id_3
    - content: Save the artifacts to a location accessible by your company’s compute infrastructure.
      id: id_4
```

`@sct`
```{python}
checks:
  - condition: check_index(id_0) == solution
    incorrectMessage: "You should always start by getting the latest version of the code."
  - condition: check_index(id_1) == solution
    incorrectMessage: "There’s not much you can do with a Python package if it depends on other packages and modules that are not in the standard library. In this case, you must install such dependencies prior to running the code."
  - condition: check_index(id_2) == solution
    incorrectMessage: "Tests can be run once you have the package’s dependencies. While you can run them after creating artifacts, it’s typically wasteful to do so, if the tests don’t succeed."
  - condition: check_index(id_3) == solution
    incorrectMessage: "When the tests succeed, you have some sense of certainty that your code will do what it was meant to. So now you can create a package."
  - condition: check_index(id_4) == solution
    incorrectMessage: "This is one of the last steps in creating software."
successMessage: "Well done! This order is correct, because you can’t save artifacts, if you didn’t create them. Likewise, you should’t create them if the code fails tests. And for running those tests, you would need to install the dependencies of this package, as your assumption is that a clean machine. Finally, to install the dependencies, you would need to check out the code, because they’re always mentioned there with good code. If you can reason about it like this, you can implement this in various CI/CD tools by following their syntax guidelines."
isOrdered: true
```

---

## Understanding the output of pytest

```yaml
type: ExplorableExercise
key: bbee6ed322
xp: 50
```

How many unit tests are automatically discovered by `pytest` in the _workspace/spark\_pipelines/pydiaper_ project?

`@possible_answers`
- 12
- 9
- 5
- 4
- 3

`@hint`
- Navigate to the _workspace/spark\_pipelines/pydiaper_ project folder with `cd`. Once there, run `pytest .`

`@pre_exercise_code`
```{python}
import os
import shutil

from pathlib import Path

workspace = Path.home() / "workspace" 
diaper_project_path = Path("/tmp/dependencies/exercises_chapter3/spark_app")

shutil.rmtree(workspace, ignore_errors=True)
shutil.copytree(diaper_project_path, workspace / "spark_pipelines" / "pydiaper")
    
os.chdir(workspace)
from exercise_helpers import display
display('terminal/')
```

`@sct`
```{python}
msg5 = "That’s right. There are 3 tests written. One of them is in a bad shape, and we should improve the code."
msg4 = "No. Scroll a bit higher in the output of pytest. Did you confuse the warning message with an actual test, perhaps?"
msg3 = "No. Scroll a bit higher in the output of pytest. "
msg2 = "No. Scroll a bit higher in the output of pytest. "
msg1 = "No. Scroll a bit higher in the output of pytest. "

Ex().has_chosen(5, [msg1, msg2, msg3, msg4, msg5])
```

---

## Improving style guide compliancy

```yaml
type: IDEExercise
key: 00bd8f0323
xp: 100
```

One of the reasons why Python is an easy language to get into, is because there’s a lot of similarity between code from different developers. That’s because the Python syntax relies on indents for scoping rules. Many developers follow the style guide of Python, known as PEP8. 

In the project you've been building so far, you want to enforce that people follow the rules laid out in PEP8. You can do so with Flake8, which is a _static_ code checker (it does not _run_ your code). You run `flake8` in the same way that you run `pytest`. It will show warnings and errors for code that is not compliant with PEP8.

In this exercise, you need to focus only on the _~/workspace/spark\_pipelines/pydiaper_.

`@instructions`
- Add `flake8` to the development section in the _Pipfile_, which is in the project’s root folder. This file serves a similar purpose as the _requirements.txt_ files you might have seen in other Python projects. It solves some problems with those though. To add `flake8` correctly, look at the line that mentions `pytest`.
- Add `flake8` to the _.circleci/config.yml_ just before the line that triggers a run of `pytest`. Make sure to duplicate the syntax with `pipenv run`.
- Finally, execute `pipenv run flake8 .` in the project’s root folder to see how many errors and warnings were generated. This is what CircleCI would execute for you and it could stop executing other steps, because this command generates errors.

`@hint`
- You only need to modify two text files. Each modification is the addition of a single line.
- You can derive the syntax of the line by looking at the configuration of `pytest` in the same file. `pytest` and `flake8` are invoked in the same way.

`@pre_exercise_code`
```{python}
import os
import shutil
from pathlib import Path


workspace = Path.home() / "workspace" 
diaper_project_path =  Path("/tmp/dependencies/exercises_chapter3/spark_app")

#shutil.rmtree(workspace, ignore_errors=True)
shutil.rmtree(workspace / "spark_pipelines" / "pydiaper" ,#/".circle" / "config.yml",
              ignore_errors=True)  # the sample_code is apparently installed before the PEC here?
shutil.copytree(diaper_project_path, workspace / "spark_pipelines" / "pydiaper")
```

`@sample_code`
```{python}
- path: spark_pipelines/pydiaper/.circleci/config.yml
  open: true
  content: |
    version: 2
    jobs:
      build:
        working_directory: ~/data_scientists/optimal_diapers/
        docker:
          - image: gcr.io/my-companys-container-registry-on-google-cloud-123456/python:3.6.4
        steps:
          - checkout
          - run:
              command: |
                sudo pip install pipenv
                pipenv install
          - run:
              command: |
                pipenv run pytest .
          - store_test_results:
              path: test-results
          - store_artifacts:
              path: test-results
              destination: tr1
- path: spark_pipelines/pydiaper/Pipfile
  open: true
  content: |
    [[source]]
    name = "pypi"
    url = "https://pypi.org/simple"
    verify_ssl = true
    
    [dev-packages]
    pyspark-stubs = ">=2.4.0"
    pytest = "*"
    
    [packages]
    pyspark = ">=2.4.0"
    
    [requires]
    python_version = "3.6"
```

`@solution`
```{python}
- path: spark_pipelines/pydiaper/.circleci/config.yml
  open: true
  content: |
    version: 2
    jobs:
      build:
        working_directory: ~/data_scientists/optimal_diapers/
        docker:
          - image: gcr.io/my-companys-container-registry-on-google-cloud-123456/python:3.6.4
        steps:
          - checkout
          - run:
              command: |
                sudo pip install pipenv
                pipenv install
          - run:
              command: |
                pipenv run flake8 .
                pipenv run pytest .
          - store_test_results:
              path: test-results
          - store_artifacts:
              path: test-results
              destination: tr1
- path: spark_pipelines/pydiaper/Pipfile
  open: true
  content: |
    [[source]]
    name = "pypi"
    url = "https://pypi.org/simple"
    verify_ssl = true
    
    [dev-packages]
    pyspark-stubs = ">=2.4.0"
    pytest = "*"
    flake8 = "*"
    
    [packages]
    pyspark = ">=2.4.0"
    
    [requires]
    python_version = "3.6"
```

`@sct`
```{python}
# Students need to add two lines. It's really simple. And yet, many will find this challenging. Welcome to Data Engineering in a DevOps world.
# @SCT writer: I propose this gets tested by simply diffing the files with the correct solution. The solution files are kept in another location, see the pre-exercise code.

#OLIVER Solution: in pipfile, add `flake8 = "*"` anywhere in the dev-packages section
# Solution second step, in config_yml: `pipenv run flake8 .`

# SCT Use configlib to check the different sections (the development packages) and check if the package was added to that section. For config, regex should be fine.
# https://docs.python.org/3/library/filecmp.html

#TODO Can't check the file, connection error. Can we implement the diffing suggested by Oliver above?

# TODO: remove when added in protowhat
def prepare_validation(state):
  if state.force_diagnose:
    import subprocess
    subprocess.run(['/bin/bash', '-i', '-c', "pipenv run flake8 ."])
Ex() >> prepare_validation

from yaml import safe_load
from pathlib import Path
from toml import load
import re

diaper_dir = Path("/home/repl/workspace/spark_pipelines/pydiaper")
try:
	pip_value = load(diaper_dir / "Pipfile")["dev-packages"]
except:
    Ex().fail("Your Pipfile syntax isn’t correct")

if pip_value.get("flake8") is None:
  Ex().fail("It seems that you did not add `flake8` in the correct section of the _Pipfile_. Check the _Pipfile_ and look at how `pytest` is added: you should follow the same pattern.")
elif pip_value["flake8"] != "*":
  Ex().fail("It seems that you added `flake8` in the expected location. However, it appears you did not give it the appropriate value.")
  
try:
	config_value = safe_load((diaper_dir / ".circleci" / "config.yml").read_text())["jobs"]["build"]["steps"][2]["run"]["command"]
except:
    Ex().fail("Your CircleCI config syntax isn’t correct")

pattern = r'pipenv\s+run\s+flake8\s+\.'

if re.search(pattern, config_value) is None:
  Ex().fail("It seems that you did not add the command to run a `flake8` test. Check _.circleci/config.yml_ and look at how `pytest` is triggered: you should follow the same pattern.")

success_msg("Neat! Now this was easy, but remember that any tool for continuous integration has many knobs. Make sure to read up on some documentation for whichever CI tool is being used in your organization.")
```
