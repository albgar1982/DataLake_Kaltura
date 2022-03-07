Feature: Fact Catalogue

  Scenario: Table computation
  Scenario: Check the Schema
    Given the "fact_catalogue" with the Data Lake "pro", "dsds" and "default" config is to be evaluated
    When we read the data
    And we check the schema
    Then we check the columns defined by product
    And we check the adhoc columns
    And check that the formatting for the column names is correct
    And we check that there are no columns that are not covered by the knowledge layer
  Scenario: Check the data
    When check the data in each column
    Then hope that there are no null values
    And we expect default values to exist
  Scenario: Data Check
    Then we check the value of columns
  Scenario: Business rules
    Then evaluate the column fulltitle of "fact_catalogue" has contents
    And evaluate the column contentduration is in milliseconds
