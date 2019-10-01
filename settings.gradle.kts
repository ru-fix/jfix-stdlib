rootProject.name = "jfix-stdlib"

Projs.values().forEach {
    include(it.directory)
}
