Feature: PlaybackActivity

  Scenario: Table computation
  Scenario: Check the Schema
    Given the "playbackactivity" with the Data Lake "pro", "dsds" and "default" config is to be evaluated
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
    Then evaluate the column fulltitle of "playbackactivity" has contents
  Scenario: Raw VS Data Warehouse
    When check that there are a total of 17 playbacks
    And the total number of playbacks per day will be checked
      |daydate:String|quantity:Integer|
      |20220212|1|
      |20220205|1|
      |20220204|3|
      |20220202|2|
      |20220123|1|
      |20220112|1|
      |20220103|2|
      |20220102|4|
      |20210130|2|

    And evaluate some playbacks
      |brandid:String|userid:String|subscriptionid:String|producttype:String|yeardate:String|monthdate:String|hourdate:String|daydate:String|contentid:String|serieid:String|contenttype:String|consumptiontype:String|countrycode:String|regioncode:String|title:String|channel:String|operator:String|serietitle:String|season:String|episode:String|fulltitle:String|genres:String|playbacktime:Integer|contentduration:Integer|origin:String|device:String|devicetype:String|network:String|viewerid:String|
      |285df710-355f-11eb-9917-e71a95af48ce|f5e6d03e-6681-4a8a-8942-8602a4c42a97|NA|NA|20220101|20220201|2022020500|20220204|ITEM#HDTL2005|COLLECTION#HDTL2005|episode|SVOD|po|po-vr|Piramid|NA|NA|Gravity Falls|2|14|Gravity Falls S-2 E-14 Piramid|Animated|70000|1920000|ACCEDO ONE|iPadOS|Tablet|NA|NA|
      |285df710-355f-11eb-9917-e71a95af48ce|004d25cc-5570-4609-acb5-d794444d5abf|NA|NA|20220101|20220101|2022010307|20220103|ITEM#MVMZ0004|NA|VIDEO|SVOD|es|es-md|Love Surreal|NA|NA|NA|NA|NA|Love Surreal|Romance|120000|5002000|ACCEDO ONE|NA|NA|NA|NA|
      |285df710-355f-11eb-9917-e71a95af48ce|004d25cc-5570-4609-acb5-d794444d5abf|NA|NA|20220101|20220101 |2022010211|20220102|ITEM#MVMZ0001|NA|VIDEO|SVOD|us|us-tx|Edge of the Garden|NA|NA|NA|NA|NA|Edge of the Garden|Drama,Romance,Comedy|0|5267000|ACCEDO ONE|Browser|WEB|NA|NA|
      |285df710-355f-11eb-9917-e71a95af48ce|004d25cc-5570-4609-acb5-d794444d5abf|NA|NA|20220101|20220101|2022010300|20220102|ITEM#MVMZ0003|COLLECTION#MVMZ0003|episode|SVOD|us|NA|Just Kiss|NA|NA|Outlander|2|4|Outlander S-2 E-4 Just Kiss|Romance,Drama|1000|2788000|ACCEDO ONE|FireOS|TV|NA|NA|

    Then check the device and devicetype
      |device:String|devicetype:String|quantity:Integer|
      |iPadOS|Tablet|1|
      |Android|TV|2|
      |Browser|WEB|1|
      |Linux|TV|1|
      |FireOS|NA|1|
      |Android|Phone|1|
      |FireOS|TV|1|
      |Chrome OS|Desktop|1|
      |iOS|Phone|1|
      |Chromecast|NA|1|
      |Apple TV|TV|1|
      |ROKU|TV|1|

    And check the userid is according to contentid
      |userid:String|contentid:String|
      |f5e6d03e-6681-4a8a-8942-8602a4c42a97|ITEM#HDTL2005|
      |bb92f273-38d6-441c-9f3d-e5f0b4159b98|ITEM#MFSR0103|
      |3b92f223-38d6-221c-9f3d-e5f0b1129b98|ITEM#HTDL0007|
      |004d25cc-5570-4609-acb5-d794444d5abf|ITEM#MVMZ0001|
      |c9ffaba2-a810-40fa-a0b3-2a9e11cf63cc|ITEM#HDTL2002|
      |1ac0814f-9ce1-4638-87ba-30841e344e16|ITEM#CECO5043|
      |15626666-e4ad-4e7f-abd0-06a2bcf8f8f1|ITEM#HDTL3013|
      |8ccf4f74-cf7d-4ef8-bd60-34cc00c480ca|ITEM#OSMZ1008|
      |45d41973-959e-49fd-ab4e-51cdd9976d77|ITEM#HDTL2001|
      |6b2fed2d-9d6e-4173-b1ec-aa7a59e050d5|ITEM#HDTL2004|
      |4e39a23e-ffd5-4f4d-a6f6-e5c394bb82b1|ITEM#GWTH5005|
      |eee05484-7139-4141-ab63-2867a0edccb1|ITEM#HDTL2003|
      |004d25cc-5570-4609-acb5-d794444d5abf|ITEM#MVMZ0002|

    And check that the serieid is according to contentid and contenttype in playbacks
      |contentid:String|serieid:String|contenttype:String|
      |ITEM#OSMZ1008|COLLECTION#OSMZ1008|episode|
      |ITEM#HDTL2005|COLLECTION#HDTL2005|episode|
      |ITEM#HDTL2002|COLLECTION#HDTL2002|episode|
      |ITEM#MVMZ0003|COLLECTION#MVMZ0003|episode|
      |ITEM#HDTL2001|COLLECTION#HDTL2001|episode|
      |ITEM#HTDL1080|COLLECTION#HTDL1080|episode|
      |ITEM#GWTH5005|COLLECTION#GWTH5005|episode|
      |ITEM#CECO5043|COLLECTION#CECO5043|episode|