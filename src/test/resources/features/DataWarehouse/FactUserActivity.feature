Feature: Fact UserActivity

  Sample text explanation

  Scenario: Table computation
  Scenario: Configuration
    Given the "fact_useractivity" with the Data Lake "pro", "dsds" and "default" config is to be evaluated
    When we read the data
    Then we obtain the metadata with spark
    #And we check the schema
  Scenario: Check the Schema
    Then we check the columns defined by product
    And we check the adhoc columns
    And check that the formatting for the column names is correct
    And we check that there are no columns that are not covered by the knowledge layer
  Scenario: Data Check
    Then we check the value of columns
  Scenario: Check Business rules
    Then evaluate paying subscription events have been generated properly
      |eventtype:String|event:String|status:String|
      |StateChange|PayingSubscription|PayingSubscription|
      |Start|Subscription|PayingSubscription            |
      |End|Subscription  |PayingSubscription            |
      |Start|Suspension|PayingSubscription              |
      |End|Suspension|PayingSubscription                |

    And evaluate trial subscription events have been generated properly
      |eventtype:String|event:String|status:String|
      |StateChange|TrialSubscription|TrialSubscription|
      |Start|Subscription|TrialSubscription|
      |End|Subscription|TrialSubscription  |
      |Start|Suspension|PayingSubscription              |
      |End|Suspension|PayingSubscription                |

    And evaluate registration events have been generated properly
      |eventtype:String|event:String|status:String|
      |Start|Registration|NA|
      |End|Registration|NA  |


