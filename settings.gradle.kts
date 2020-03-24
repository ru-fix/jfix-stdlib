rootProject.name = "jfix-stdlib"

for (project in listOf(
        "jfix-stdlib-concurrency",
        "jfix-stdlib-files",
        "jfix-stdlib-id-generator",
        "jfix-stdlib-id-generator-jmh",
        "jfix-stdlib-ratelimiter",
        "jfix-stdlib-socket")) {
    include(project)
}
