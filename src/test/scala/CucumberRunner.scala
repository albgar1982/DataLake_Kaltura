import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
// https://cucumber.io/docs/cucumber/api/
@CucumberOptions(
  plugin = Array("pretty", "html:target/cucumber/html"),
  glue = Array("dataLake/core/tests/cucumber"),
  //tags = Array("not @Wip"),
)
class CucumberRunner {}