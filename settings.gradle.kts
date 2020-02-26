rootProject.name = "jfix-stdlib"

for (project in listOf(
        "jfix-stdlib-concurrency",
        "jfix-stdlib-files",
        "jfix-stdlib-id_generator",
        "jfix-stdlib-id_generator_jmh",
        "jfix-stdlib-ratelimiter",
        "jfix-stdlib-socket")) {
    include(project)
}
