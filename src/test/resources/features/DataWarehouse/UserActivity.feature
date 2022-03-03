Feature: UserActivity

  Scenario: Table computation
  Scenario: Check the Schema
    Given the "useractivity" with the Data Lake "pro", "dsds" and "default" config is to be evaluated
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
  #Scenario: Check the Columns Values
    #Then we check the value of columns
  #Scenario: Product Business Rules
  Scenario: Raw VS Data Warehouse
    When check that there are a total of 118 events
    And check that there are a total of 24 users
    And the total number of events per day will be checked
      |daydate:String|quantity:Integer|
      |20191122|8|
      |20210303|1|
      |20200101|4|
      |20200202|8|
      |20220219|33|
      |20201230|7|
      |20201101|1|
      |20200303|1|
      |20200808|4|
      |20200820|1|
      |20220220|10|
      |20200422|6|
      |20201025|1|
      |20211211|1|
      |20200505|4|
      |20190929|4|
      |20190928|1|
      |20220219|33|
      |20220220|10|
      |20210121|4|
      |20200825|4|
      |20200901|4|
      |20200930|2|
      |20201031|1|
      |20210218|1|
      |20210319|2|
      |20210501|1|
      |20210512|1|
      |20210831|1|
      |20210916|1|
      |20211111|1|

    And the total number of events per user
      |userid:String|quantity:Integer|
      |502200000|4|
      |510000001|5|
      |510000002|4|
      |510000003|4|
      |510000004|4|
      |510000005|5|
      |510000006|5|
      |510000007|5|
      |510000008|4|
      |510000009|6|
      |510000010|5|
      |510000011|4|
      |510000012|4|
      |510000013|11|
      |510000015|5|
      |510000016|5|
      |510000017|4|
      |510000018|5|
      |510000019|4|
      |510000020|6|
      |510000021|4|
      |510000022|5|
      |510000023|5|
      |510000024|5|

    Then check the possible events logic between event, eventtype and status
      |event:String|eventtype:String|status:String|quantity:Integer|
      |NA|ProductTypeChange|PayingSubscription|1|
      |Registration|Start|NA|25|
      |Registration|End|NA|10|
      |Subscription|Start|PayingSubscription|25|
      |Standard|ProductTypeChange|PayingSubscription|24|
      |PayingSubscription|StateChange|PayingSubscription|25|
      |Subscription|End|PayingSubscription|8|

    And evaluate some users
      |userid:String|status:String|eventtype:String|event:String|cancellationtype:String|producttype:String|monthdate:String|yeardate:String|paymentmethod:String|subscriptionid:String|gender:String|birth:String|country:String|countrycode:String|region:String|regioncode:String|email:String|operator:String|brandid:String|daydate:Integer|
      |510000013|PayingSubscription|Start            |Subscription      |NA              |Standard   |20220201 |20220101|Roku         |HMN Annual Roku - 506061        |unspecified|NA      |united states|us         |Maryland      |us-md     |NA   |NA      |285df710-355f-11eb-9917-e71a95af48ce|20220219|
      |510000003|PayingSubscription|Start            |Subscription      |NA              |Standard   |20220201 |20220101|NA           |HMN Annual Roku - 506061        |female     |20011123|united states|us         |Georgia       |us-ga     |NA   |NA      |285df710-355f-11eb-9917-e71a95af48ce|20220219|
      |510000013|PayingSubscription|Start            |Subscription      |NA              |Standard   |20220201 |20220101|Offline      |Offline - 507285                |unspecified|NA      |united states|us         |Maryland      |us-md     |NA   |NA      |285df710-355f-11eb-9917-e71a95af48ce|20220219|
      |510000008|PayingSubscription|Start            |Subscription      |NA              |Standard   |20220201 |20220101|CreditCard   |MONTHLY - 506003                |unspecified|19011111|NA           |NA         |NA            |NA        |NA   |NA      |285df710-355f-11eb-9917-e71a95af48ce|20220219|
      |510000009|PayingSubscription|Start            |Subscription      |NA              |Standard   |20220201 |20220101|Offline      |ANNUAL - 506055                 |NA         |19970717|united states|us         |Michigan      |us-mi     |NA   |NA      |285df710-355f-11eb-9917-e71a95af48ce|20220219|
      |510000020|PayingSubscription|Start            |Subscription      |NA              |NA         |20220201 |20220101|Roku         |ANNUAL - 506055                 |male       |20000131|united states|us         |Maine         |us-me     |NA   |NA      |285df710-355f-11eb-9917-e71a95af48ce|20220219|
      |510000024|PayingSubscription|Start            |Subscription      |NA              |Standard   |20220201 |20220101|Offline      |Offline - 506061                |male       |19800211|united states|us         |Florida       |us-fl     |NA   |NA      |285df710-355f-11eb-9917-e71a95af48ce|20220219|
      |510000023|PayingSubscription|Start            |Subscription      |NA              |Standard   |20220201 |20220101|GooglePlay   |HMN Annual Roku - 507581        |female     |19501231|united states|us         |Arizona       |us-az     |NA   |NA      |285df710-355f-11eb-9917-e71a95af48ce|20220219|

    Then check that the paymentmethod receives all the methods
      |paymentmethod:String|quantity:Integer|
      |NA           |1       |
      |GooglePlay   |4       |
      |iTunes       |4       |
      |NotSet       |3       |
      |Roku         |5       |
      |Amazon       |1       |
      |Offline      |2       |
      |CreditCard   |4       |

    And check the total of cancellationtypes
      |cancellationtype:String|quantity:Integer|
      |User|6|
      |Service|2|

    And check the total of producttypes
      |producttype:String|quantity:Integer|
      |NA         |14      |
      |Standard   |10      |

    And check the total of genders
      |gender:String|quantity:Integer|
      |male|7|
      |female|7|
      |unspecified|5|
      |NA|5|

    And evaluate the regions and country codes
      |country:String|countrycode:String|region:String|regioncode:String|quantity:Integer|
      |united states|us         |Texas         |us-tx     |1       |
      |united states|us         |Maryland      |us-md     |2       |
      |united states|us         |Michigan      |us-mi     |1       |
      |NA           |NA         |NA            |NA        |2       |
      |united states|us         |New Jersey    |us-nj     |1       |
      |united states|us         |Florida       |us-fl     |1       |
      |united states|us         |South Carolina|us-sc     |1       |
      |united states|us         |Wisconsin     |us-wi     |2       |
      |NA           |us         |NA            |NA        |2       |
      |united states|us         |Maine         |us-me     |2       |
      |united states|us         |Idaho         |us-id     |1       |
      |united states|us         |California    |us-ca     |1       |
      |united states|us         |Missouri      |us-mo     |1       |
      |united states|us         |Georgia       |us-ga     |2       |
      |united states|us         |Nevada        |us-nv     |1       |
      |united states|us         |Arizona       |us-az     |2       |
      |united states|us         |Ohio          |us-oh     |1       |

    And check the birthday dates
      |birth:String|quantity:Integer|
      |19801111|1       |
      |20011123|1       |
      |19920212|1       |
      |19800211|1       |
      |20010309|1       |
      |NA      |4       |
      |19920228|1       |
      |19950609|1       |
      |20020818|1       |
      |19950819|1       |
      |19011111|1       |
      |19970717|1       |
      |19101031|1       |
      |19671225|1       |
      |20000830|1       |
      |19780101|1       |
      |20000131|1       |
      |19850606|1       |
      |19100503|1       |
      |19890214|1       |

    Then evaluate the subscriptionid
      |subscriptionid:String|quantity:Integer|
      |Offline - 506061                |1|
      |HMN Annual Roku - 506061        |1|
      |HMN Monthly Google Play - 507292|1|
      |NA - 506061                     |1|
      |MONTHLY - 507290                |1|
      |HMN Monthly iTunes - 506056     |1|
      |HMN Monthly Google Play - 507290|1|
      |NA                              |1|
      |Offline - 507285                |1|
      |MONTHLY - 506003                |1|
      |MONTHLY - 0                     |1|
      |HMN Annual Roku - 507581        |1|
      |ANNUAL - 506055                 |1|
      |MONTHLY - 506061                |1|
      |HMN Annual Roku - 507285        |1|
      |NA - 506060                     |1|
