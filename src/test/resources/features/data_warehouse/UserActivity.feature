Feature: Fact UserActivity

  Sample text explanation

  Scenario: Table computation
  Scenario: Configuration
    Given the "fact_useractivity" with the Data Lake "pro", "dsds" and "default" config is to be evaluated
    When we read the data with spark
    Then we obtain the metadata with spark
  Scenario: Check the Schema
    Then we check the columns defined by product
    And we check the adhoc columns
    And check that the formatting for the column names is correct
    And we check that there are no columns that are not covered by the knowledge layer
  Scenario: Data Check
    Then we check the value of columns