import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
// https://cucumber.io/docs/cucumber/api/
@CucumberOptions(
  plugin = Array("pretty", "html:target/cucumber/html"),
  features = Array(
    "classpath:features/1_Setup.feature",
    "classpath:features/DataWarehouse/FactUserActivity.feature",
    "classpath:features/DataWarehouse/FactCatalogue.feature",
    "classpath:features/DataWarehouse/PlaybackActivity.feature"
  ),
  //tags = Array("not @Wip"),
)
class CucumberRunner {

}