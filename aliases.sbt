import Util._

addCommandAlias("c", "compile")
addCommandAlias("ca", "test:compile")
addCommandAlias("t", "test")
addCommandAlias(
  "up2date",
  "reload plugins; dependencyUpdates; reload return; dependencyUpdates"
)

onLoadMessage +=
  s"""|
      |───────────────────────────
      |  List of defined ${styled("aliases")}
      |────────┬──────────────────
      |${styled("c")}       │ compile
      |${styled("ca")}      │ compile all
      |${styled("t")}       │ test
      |${styled("up2date")} │ dependencyUpdates
      |────────┴──────────────────""".stripMargin
