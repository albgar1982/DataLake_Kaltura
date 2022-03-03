Feature: Catalogue

  Scenario: Table computation
  Scenario: Check the Schema
    Given the "catalogue" with the Data Lake "pro", "dsds" and "default" config is to be evaluated
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
  Scenario: Check the Columns Values
    Then we check the value of columns
  Scenario: Product Business Rules
    Then evaluate the column fulltitle of "catalogue" has contents
    #And evaluate the column contentduration is in milliseconds
  Scenario: Raw VS Data Warehouse
    When check that there are a total of 17 catalogues
    And the total number of catalogues per day will be checked
      |daydate:String|quantity:Integer|
      |20201102|1|
      |20220210|1|
      |20220205|1|
      |20220204|3|
      |20220202|3|
      |20220122|1|
      |20220130|1|
      |20220103|2|
      |20220102|4|

    And evaluate some catalogues
      |brandid:String|contentid:String|serieid:String|daydate:String|title:String|contenttype:String|contentduration:Integer|season:String|episode:String|consumptiontype:String|network:String|genres:String|countrycode:String|regioncode:String|serietitle:String|fulltitle:String|channel:String|origin:String|synopsis:String|
      |285df710-355f-11eb-9917-e71a95af48ce|ITEM#OSMZ1008|COLLECTION#OSMZ1008|20220103|A Shadow in His Dream|episode|2846000|3|3|SVOD|NA|Drama|NA|NA|The Witch|The Witch S-3 E-3 A Shadow in His Dream|NA|ACCEDO ONE|Description of the movie or serie...|
      |285df710-355f-11eb-9917-e71a95af48ce|ITEM#MVMZ0232|NA|20220122|Love Surreal|VIDEO|4885000|NA|NA|SVOD|NA|Romance|NA|NA|NA|Love Surreal|NA|ACCEDO ONE|Description of the movie or serie...|
      |285df710-355f-11eb-9917-e71a95af48ce|ITEM#GWTH5005|COLLECTION#GWTH5005|20220102|2 Days Left|episode|2800000|9|7|SVOD|NA|Comedy,Drama|NA|NA|The Miracle Of Christmas Eve's|The Miracle Of Christmas Eve's S-9 E-7 2 Days Left|NA|ACCEDO ONE|Description of the movie or serie...|
      |285df710-355f-11eb-9917-e71a95af48ce|ITEM#HDTL2003|NA|20220204|Titanic|VIDEO|8900000|NA|NA|SVOD|NA|Romance,Drama|NA|NA|NA|Titanic|NA|ACCEDO ONE|Description of the movie or serie...|

    Then check that the serieid is according to contentid and contenttype in catalogues
      |contentid:String|serieid:String|contenttype:String|
      |ITEM#OSMZ1008|COLLECTION#OSMZ1008|episode|
      |ITEM#HDTL2005|COLLECTION#HDTL2005|episode|
      |ITEM#HDTL2002|COLLECTION#HDTL2002|episode|
      |ITEM#MVMZ0003|COLLECTION#MVMZ0003|episode|
      |ITEM#HDTL2001|COLLECTION#HDTL2001|episode|
      |ITEM#HTDL1080|COLLECTION#HTDL1080|episode|
      |ITEM#GWTH5005|COLLECTION#GWTH5005|episode|
      |ITEM#CECO5043|COLLECTION#CECO5043|episode|
      |ITEM#CECO1005|COLLECTION#CECO1005|episode|