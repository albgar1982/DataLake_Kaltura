import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
// https://cucumber.io/docs/cucumber/api/
@CucumberOptions(
  plugin = Array("pretty", "html:target/cucumber/html"),
  features = Array(
    "classpath:features/1_Setup.feature",
    "classpath:features/DataWarehouse/PlaybackActivity.feature",
    "classpath:features/DataWarehouse/Catalogue.feature",
    "classpath:features/DataWarehouse/UserActivity.feature"
  ),
  //tags = Array("not @Wip"),
)
class CucumberRunner {}