# 📺 SUSTech Database System Project 2

![Course](https://img.shields.io/badge/sustech-cs307%2F213-blue)
![Project2](https://img.shields.io/badge/23_fall-project_2-green)

Database course project. Backend for video websites.
[*[Read my report]*](Report_12210729.pdf)

## Prerequisites

- **JDK**: This template requires JDK 8+ and is compatible up to Java 20. We will use _JDK 20_ to work around your
  submitted jar file.

  > Other JVM languages (Kotlin, Groovy) are supported as long as you build a jar file that can pass the local
  benchmark.

- **Database**: You need to host your own PostgreSQL database when developing. We will use _PostgreSQL 16_ to run
  the final benchmark.

  > As we may require you to return the sorted result in some query, please create the database with `LC_COLLATE='C'`
  option, which provides the platform-independent sorting result.
  > ```sql
  > CREATE DATABASE sustc WITH ENCODING = 'UTF8' LC_COLLATE = 'C' TEMPLATE = template0;
  >```

## Getting started


### Understand the project structure

<details>
<summary><b>Expand this section to see the sample code tree</b></summary>
<pre>
.
├── sustc-api                                              # You should work around this directory
│   ├── build.gradle.kts                                   # You may add any utility libraries here
│   └── src/main/java
│                └── io.sustc
│                    ├── dto                               # The passed arguments and return values of services are defined as DTO (Data Transfer Object)
│                    │   └... User.java                    # You should not modify the defined fields or methods, but you may add new ones
│                    ├── exceptions                        # You should throw correct exceptions as instructed in JavaDoc
│                    │   └... EntityNotFoundException.java
│                    └── service                           # The interfaces of services are defined here
│                        ├... DatabaseService.java
│                        └── impl                          # We suggest you to place your implementation here. Actually, any package is OK, as long as annotating the class as `@Service`
│                            └... DatabaseServiceImpl.java
└── sustc-runner                                           # We've provided you an interactive command line runner, a benchmark runner with sample data, and unit tests
    ├── compose.yml                                        # You may optionally use a container-based database to prevent polluting your local database
    ├── data                                               # Contains records to import and test cases
    └── src/main
            ├── java
            │   └── io.sustc
            │       └── command                            # You may call the services here, type `help` in shell to list all commands
            │           └... UserCommand.java
            └── resources
                └── application.yml                        # Configure your database connection and benchmark behavior here
</pre>

You don't need to care about the unlisted files and directories.
</details>

The project is managed by Gradle to help you manage dependencies, run locally, and build the submission.

- [`sustc-api`](sustc-api) is the module where you should implement your services.
- [`sustc-runner`](sustc-runner) module provides you an interactive command line runner and a benchmark runner with sample data.

Basically, you only need to focus on three directories:

1. [`io.sustc.service`](sustc-api/src/main/java/io/sustc/service) package contains the interfaces of services you
   need to implement. You
   should not modify the interfaces. A demo implementation is provided in
   [`DatabaseServiceImpl`](sustc-api/src/main/java/io/sustc/service/impl/DatabaseServiceImpl.java).
   **Please also update the `getGroupMembers` method to help us identify your submission.**

2. [`io.sustc.dto`](sustc-api/src/main/java/io/sustc/dto) package contains the definitions of DTOs (Data Transfer
   Objects) that act as the arguments and return values of services. You should not modify the defined fields or
   methods, but you may add new ones.

3. [`application.yml`](sustc-runner/src/main/resources/application.yml) is the configuration file for running the
   program. You may configure your database connection and benchmark behavior here.

Please note that your submission is built by the `sustc-api` module, any modification to the `sustc-runner` module will
not take effect in the final benchmark.

### Run locally

After setting up your database and modifying the configuration file, you may run the program locally in two modes.

#### 3.1 Interactive shell

You can test your services by typing commands in any sequence, with any data. Run the Gradle task named **`bootRun`**
to start the interactive shell.

```shell
./gradlew :sustc-runner:bootRun  # or simply   ./gradlew bootRun
```

> If you are using IntelliJ IDEA, you may find the list of Gradle tasks on the right side of the window.
<p align="center"><img src=".github/assets/idea-gradle.png" style="width:220px"></p>

You may type `help` after the application starts up to see the list of available commands.

#### 3.2 Benchmark with sample data

[The benchmark service](sustc-runner/src/main/java/io/sustc/benchmark/BenchmarkService.java) works exactly the same as how
we will judge your submission, which grades your services based on both correctness and performance.
To run the benchmark, use the **`benchmark`** Gradle task.

Note that you need to implement all service interfaces to run the benchmark.
You may create dummy implementations (e.g. throw UnsupportedOperationException) before actually implementing them.



## Useful links

- [PostgreSQL 16 Documentation](https://www.postgresql.org/docs/16/index.html)
    - [Backward Compatibility](https://www.postgresql.org/docs/16/runtime-config-compatible.html)
- [SpringBoot 2.7 Documentation](https://docs.spring.io/spring-boot/docs/2.7.16/reference/htmlsingle/)
- [Lombok Documentation](https://projectlombok.org/features/)
- [Gradle User Manual](https://docs.gradle.org/8.3/userguide/userguide.html)
    - [Managing dependencies with Gradle](https://www.baeldung.com/gradle#plugins)

## License

The project is designed for educational purpose for the course _CS307 Principles of Database Systems_ and
_CS213 Principles of Database Systems (H)_ at Southern University of Science and Technology.

The code template is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
