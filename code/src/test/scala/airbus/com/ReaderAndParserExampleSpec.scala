package airbus.com

import org.scalatest.{FunSpec, GivenWhenThen, Matchers}
import org.specs2.matcher.describe.SeqIdentical

import scala.jdk.CollectionConverters._

class ReaderAndParserExampleSpec
  extends FunSpec
    with GivenWhenThen
    with SparkSessionTestWrapper
    with Matchers
    with ReaderAndParser {

  spark.sparkContext.setLogLevel("ERROR")

  val accDF = extractDataFrameFromFile(spark, "src/test/resources/EXAMPLE_XML.xml")
  val list1DF = flattenFileToCreateList(accDF)

  describe("The reader and parser") {

    it("should extract all the columns for the list  (field1, field2)") {

      Given("2 elements (1 with 3 sched_lru elements and 1 with 2 sched_lru elements, such as the followings: " +
        "\n<maintenance_task task_number=\"TASK_1\" revision_number=\"0024\" revision_code=\"R\" data_module_code=\"AJ-A-00-00-00-00AAA-000A-A\" ata6=\"200415\">\n\t\t\t<task_code>GVI</task_code>\n\t\t\t<task_title>Tarea 1 </task_title>\n\t\t\t<task_description>Tarea 1 description</task_description>\n\t\t\t<sched_lru position_code=\"5001KS1\" pnr=\"PNR_28\" mfc=\"XXXXX\"/>\n\t\t\t<sched_lru position_code=\"5001KS2\" pnr=\"PNR_29\" mfc=\"XXXXX\"/>\n\t\t\t<sched_lru position_code=\"5000KS3\" pnr=\"PNR_30\" mfc=\"XXXXX\"/>\n\t\t\t<frequency ls_event_code=\"ToT/FF+90 > FF\">\n\t\t\t\t<interval>\n\t\t\t\t\t<usage_figure_unit>MO</usage_figure_unit>\n\t\t\t\t\t<interval_value>1</interval_value>\n\t\t\t\t</interval>\n\t\t\t</frequency>\n\t\t\t<applicability>\n\t\t\t\t<actype actypecode=\"A400M\"/>\n\t\t\t\t<applic>\n\t\t\t\t\t<ac_applic>\n\t\t\t\t\t\t<range from=\"1\" to=\"500\"/>\n\t\t\t\t\t\t<textual_cond>TEXTUAL CONDITIONS</textual_cond>\n\t\t\t\t\t</ac_applic>\n\t\t\t\t</applic>\n\t\t\t</applicability>\n\t\t\t<issue_type>U</issue_type>\n\t\t</maintenance_task>\n\t\t<maintenance_task task_number=\"TASK_2\" revision_number=\"0000\" revision_code=\"R\" data_module_code=\"AJ-A-00-00-00-00AAA-000A-A\" ata6=\"730000\">\n\t\t\t<task_code>RST</task_code>\n\t\t\t<task_title>Tarea 2</task_title>\n\t\t\t<task_description>Tarea 2 description</task_description>\n\t\t\t<sched_lru position_code=\"3KS1\" pnr=\"PNR_6404\" mfc=\"XXXXX\"/>\n\t\t\t<sched_lru position_code=\"3KS2\" pnr=\"PNR_6405\" mfc=\"YYYZ\"/>\n\t\t\t<frequency ls_event_code=\"CFUse\">\n\t\t\t\t<interval>\n\t\t\t\t\t<usage_figure_unit>EFH</usage_figure_unit>\n\t\t\t\t\t<interval_value>1</interval_value>\n\t\t\t\t</interval>\n\t\t\t</frequency>\n\t\t\t<applicability>\n\t\t\t\t<actype actypecode=\"A400M\"/>\n\t\t\t\t<applic>\n\t\t\t\t\t<ac_applic>\n\t\t\t\t\t\t<range from=\"1\" to=\"500\"/>\n\t\t\t\t\t\t<textual_cond>TEXTUAL CONDITIONS</textual_cond>\n\t\t\t\t\t</ac_applic>\n\t\t\t\t</applic>\n\t\t\t</applicability>\n\t\t\t<issue_type>U</issue_type>\n\t\t</maintenance_task>")
      val maintenanceTasksDF = list1DF.where("task_number == 'TASK_1' or task_number == 'TASK_2'")
      val numberOfRecords = maintenanceTasksDF.count()
      val expectedTaskNumber1 = "TASK_1"
      val expectedTaskNumber2= "TASK_2"
      val expectedPositionCode1Task1 = "5001KS1"
      val expectedPositionCode2Task1 = "5001KS2"
      val expectedPositionCode3Task1 = "5001KS3"
      val expectedPositionCode1Task2 = "3KS1"
      val expectedPositionCode2Task2 = "3KS2"
      val pnrsTasks1 = maintenanceTasksDF.where("task_number == 'TASK_1'")
      val pnrsTasks2 = maintenanceTasksDF.where("task_number == 'TASK_2'")
      val pnr1Task1PositionCode1 = maintenanceTasksDF.where("task_number == 'TASK_1' and position_code == '5001KS1'").collect()(0).getAs[String]("pnr")
      val expectedPnr1Task1PositionCode1 = "PNR_28"
      val pnr2Task1PositionCode2 = maintenanceTasksDF.where("task_number == 'TASK_1' and position_code == '5001KS2'").collect()(0).getAs[String]("pnr")
      val expectedPnr2Task1PositionCode2 = "PNR_29"
      val pnr3Task1PositionCode3 = maintenanceTasksDF.where("task_number == 'TASK_1' and position_code == '5001KS3'").collect()(0).getAs[String]("pnr")
      val expectedPnr3Task1PositionCode3 = "PNR_30"
      val pnr4Task2PositionCode1 = maintenanceTasksDF.where("task_number == 'TASK_2' and position_code == '3KS1'").collect()(0).getAs[String]("pnr")
      val expectedPnr4Task2PositionCode1 = "PNR_6404"
      val pnr5Task2PositionCode2 = maintenanceTasksDF.where("task_number == 'TASK_2' and position_code == '3KS2'").collect()(0).getAs[String]("pnr")
      val expectedPnr5Task2PositionCode2 = "PNR_6405"

      When("we parse the file")
      val elements = maintenanceTasksDF.collectAsList().asScala
      val elementsTask1 = pnrsTasks1.collectAsList().asScala
      val elementsTask2 = pnrsTasks2.collectAsList().asScala

      val customers = elements.map(e => e.getAs[String]("customer"))
      val taskNumbers = elements.map(e => e.getAs[String]("task_number"))
      val positionCodesTask1 = elementsTask1.map(e => e.getAs[String]("position_code"))
      val positionCodesTask2 = elementsTask2.map(e => e.getAs[String]("position_code"))
      val pnr = elements.map(e => e.getAs[String]("position_code"))

      Then("there should be 5 rows with the following content" +
        "\n+--------+-----------+-------------+--------+                                                         \n|customer|task_number|position_code|     pnr|                                                         \n+--------+-----------+-------------+--------+                                                         \n|   CUSTO|     TASK_1|      5001KS1|  PNR_28|                                                         \n|   CUSTO|     TASK_1|      5001KS2|  PNR_29|                                                         \n|   CUSTO|     TASK_1|      5000KS3|  PNR_30|                                                         \n|   CUSTO|     TASK_2|         3KS1|PNR_6404|                                                         \n|   CUSTO|     TASK_2|         3KS2|PNR_6405|\n+--------+-----------+-------------+--------+")
      numberOfRecords shouldBe totalPnrs
      customers should contain theSameElementsAs Seq(customerId, customerId, customerId, customerId, customerId)
      taskNumbers should contain theSameElementsAs Seq(expectedTaskNumber1, expectedTaskNumber1, expectedTaskNumber1, expectedTaskNumber2, expectedTaskNumber2)
      positionCodesTask1 should contain theSameElementsAs Seq(expectedPositionCode1Task1, expectedPositionCode2Task1, expectedPositionCode3Task1)
      positionCodesTask2 should contain theSameElementsAs Seq(expectedPositionCode1Task2, expectedPositionCode2Task2)
      pnr1Task1PositionCode1 shouldBe expectedPnr1Task1PositionCode1
      pnr2Task1PositionCode2 shouldBe expectedPnr2Task1PositionCode2
      pnr3Task1PositionCode3 shouldBe expectedPnr3Task1PositionCode3
      pnr4Task2PositionCode1 shouldBe expectedPnr4Task2PositionCode1
      pnr5Task2PositionCode2 shouldBe expectedPnr5Task2PositionCode2
    }

    it("should discard the issue_type='X' elements") {

      Given("A maintenance tasks with a issue type equals to D, such as the following: " +
        "\n<maintenance_task task_number=\"TASK_4\" revision_number=\"0033\" revision_code=\"P\" data_module_code=\"BJ-A-00-00-00-00AAA-000A-B\" ata6=\"450000\">\n\t\t\t<task_code>TRY</task_code>\n\t\t\t<task_title>Tarea 4</task_title>\n\t\t\t<task_description>Tarea 4 description</task_description>\n\t\t\t<sched_lru position_code=\"3KS1\" pnr=\"PNR_76543\" mfc=\"AAAA\"/>\n\t\t\t<sched_lru position_code=\"3KS2\" pnr=\"PNR_4321\" mfc=\"BBBB\"/>\n\t\t\t<sched_lru position_code=\"3KS3\" pnr=\"PNR_8899\" mfc=\"CCCC\"/>\n\t\t\t<sched_lru position_code=\"3KS4\" pnr=\"PNR_2234\" mfc=\"DDDD\"/>\n\t\t\t<frequency ls_event_code=\"CFUse\">\n\t\t\t\t<interval>\n\t\t\t\t\t<usage_figure_unit>EFH</usage_figure_unit>\n\t\t\t\t\t<interval_value>1</interval_value>\n\t\t\t\t</interval>\n\t\t\t</frequency>\n\t\t\t<applicability>\n\t\t\t\t<actype actypecode=\"A400M\"/>\n\t\t\t\t<applic>\n\t\t\t\t\t<ac_applic>\n\t\t\t\t\t\t<range from=\"1\" to=\"500\"/>\n\t\t\t\t\t\t<textual_cond>TEXTUAL CONDITIONS</textual_cond>\n\t\t\t\t\t</ac_applic>\n\t\t\t\t</applic>\n\t\t\t</applicability>\n\t\t\t<issue_type>D</issue_type>\n\t\t</maintenance_task>")
      val numberOfRecords = list1DF.count()
      val expectedTaskNumber1 = "TASK_1"
      val expectedTaskNumber2= "TASK_2"

      When("we parse the AMP file")
      val elements = list1DF.collectAsList().asScala
      val taskNumbers = elements.map(e => e.getAs[String]("task_number")).distinct

      Then("the maintenance should not be included in the results" +
        "\n+--------+-----------+-------------+--------+----------+\n|customer|task_number|position_code|     pnr|issue_type|\n+--------+-----------+-------------+--------+----------+\n|   CUST0|     TASK_1|      5001KS1|  PNR_28|         U|\n|   CUST0|     TASK_1|      5001KS2|  PNR_29|         U|\n|   CUST0|     TASK_1|      5001KS3|  PNR_30|         U|\n|   CUST0|     TASK_2|         3KS1|PNR_6404|         U|\n|   CUST0|     TASK_2|         3KS2|PNR_6405|         U|\n+--------+-----------+-------------+--------+----------+")
      numberOfRecords shouldBe totalPnrs
      taskNumbers should contain theSameElementsAs Seq(expectedTaskNumber1, expectedTaskNumber2)
    }

    it("should extract all the columns for the list 2 (customer, task_number, ls_event_code, usage_figure_unit)") {

      Given("3 Maintenance tasks, such as the followings: " +
        "\n<maintenance_task task_number=\"TASK_1\" revision_number=\"0024\" revision_code=\"R\" data_module_code=\"AJ-A-00-00-00-00AAA-000A-A\" ata6=\"200415\">\n\t\t\t<task_code>GVI</task_code>\n\t\t\t<task_title>Tarea 1 </task_title>\n\t\t\t<task_description>Tarea 1 description</task_description>\n\t\t\t<sched_lru position_code=\"5001KS1\" pnr=\"PNR_28\" mfc=\"XXXXX\"/>\n\t\t\t<sched_lru position_code=\"5001KS2\" pnr=\"PNR_29\" mfc=\"XXXXX\"/>\n\t\t\t<sched_lru position_code=\"5001KS3\" pnr=\"PNR_30\" mfc=\"XXXXX\"/>\n\t\t\t<frequency ls_event_code=\"ToT/FF+90 > FF\">\n\t\t\t\t<interval>\n\t\t\t\t\t<usage_figure_unit>FH</usage_figure_unit>\n\t\t\t\t</interval>\n\t\t\t\t<interval>\n\t\t\t\t\t<usage_figure_unit>MO</usage_figure_unit>\n\t\t\t\t</interval>\n\t\t\t</frequency>\n\t\t\t<applicability>\n\t\t\t\t<actype actypecode=\"A400M\"/>\n\t\t\t\t<applic>\n\t\t\t\t\t<ac_applic>\n\t\t\t\t\t\t<range from=\"1\" to=\"500\"/>\n\t\t\t\t\t\t<textual_cond>TEXTUAL CONDITIONS</textual_cond>\n\t\t\t\t\t</ac_applic>\n\t\t\t\t</applic>\n\t\t\t</applicability>\n\t\t\t<issue_type>U</issue_type>\n\t\t</maintenance_task>\n\t\t<maintenance_task task_number=\"TASK_2\" revision_number=\"0000\" revision_code=\"R\" data_module_code=\"AJ-A-00-00-00-00AAA-000A-A\" ata6=\"730000\">\n\t\t\t<task_code>RST</task_code>\n\t\t\t<task_title>Tarea 2</task_title>\n\t\t\t<task_description>Tarea 2 description</task_description>\n\t\t\t<sched_lru position_code=\"3KS1\" pnr=\"PNR_6404\" mfc=\"XXXXX\"/>\n\t\t\t<sched_lru position_code=\"3KS2\" pnr=\"PNR_6405\" mfc=\"YYYZ\"/>\n\t\t\t<frequency ls_event_code=\"CFUse\">\n\t\t\t\t<interval>\n\t\t\t\t\t<usage_figure_unit>EFH</usage_figure_unit>\n\t\t\t\t\t<interval_value>1</interval_value>\n\t\t\t\t</interval>\n\t\t\t</frequency>\n\t\t\t<applicability>\n\t\t\t\t<actype actypecode=\"A400M\"/>\n\t\t\t\t<applic>\n\t\t\t\t\t<ac_applic>\n\t\t\t\t\t\t<range from=\"1\" to=\"500\"/>\n\t\t\t\t\t\t<textual_cond>TEXTUAL CONDITIONS</textual_cond>\n\t\t\t\t\t</ac_applic>\n\t\t\t\t</applic>\n\t\t\t</applicability>\n\t\t\t<issue_type>U</issue_type>\n\t\t</maintenance_task>\n\t\t<maintenance_task task_number=\"TASK_3\" revision_number=\"0033\" revision_code=\"P\" data_module_code=\"BJ-A-00-00-00-00AAA-000A-B\" ata6=\"450000\">\n\t\t\t<task_code>TRY</task_code>\n\t\t\t<task_title>Tarea 3</task_title>\n\t\t\t<task_description>Tarea 3 description</task_description>\n\t\t\t<sched_lru position_code=\"3KS1\" pnr=\"PNR_76543\" mfc=\"AAAA\"/>\n\t\t\t<sched_lru position_code=\"3KS2\" pnr=\"PNR_4321\" mfc=\"BBBB\"/>\n\t\t\t<sched_lru position_code=\"3KS3\" pnr=\"PNR_8899\" mfc=\"CCCC\"/>\n\t\t\t<sched_lru position_code=\"3KS4\" pnr=\"PNR_2234\" mfc=\"DDDD\"/>\n\t\t\t<frequency ls_event_code=\"CFUse\">\n\t\t\t\t<interval>\n\t\t\t\t\t<usage_figure_unit>EFH</usage_figure_unit>\n\t\t\t\t\t<interval_value>1</interval_value>\n\t\t\t\t</interval>\n\t\t\t</frequency>\n\t\t\t<applicability>\n\t\t\t\t<actype actypecode=\"A400M\"/>\n\t\t\t\t<applic>\n\t\t\t\t\t<ac_applic>\n\t\t\t\t\t\t<range from=\"1\" to=\"500\"/>\n\t\t\t\t\t\t<textual_cond>TEXTUAL CONDITIONS</textual_cond>\n\t\t\t\t\t</ac_applic>\n\t\t\t\t</applic>\n\t\t\t</applicability>\n\t\t\t<issue_type>D</issue_type>\n\t\t</maintenance_task>")
      val maintenanceTasksDF = list2DF
      val numberOfRecords = maintenanceTasksDF.count()
      val eventCodesAndFigureUnitsTask1 = maintenanceTasksDF.where("task_number == 'TASK_1'")
      val eventCodesAndFigureUnitsTask2 = maintenanceTasksDF.where("task_number == 'TASK_2'")
      val expectedEventCodeTask1 = "ToT/FF+90 > FF"
      val expectedEventCodeTask2 = "CFUse"
      val expectedUsageFigureUnit1Task1 = "TASK1A"
      val expectedUsageFigureUnit2Task1 = "TASK1A2"
      val expectedUsageFigureUnit3Task1 = "TASK1B"
      val expectedUsageFigureUnit1Task2 = "TASK2A"
      val expectedUsageFigureUnit2Task2 = "TASK2B"
      val expectedUsageFigureUnit3Task2 = "YE"

      When("we parse the AMP file")
      val task1Elements = eventCodesAndFigureUnitsTask1.collectAsList().asScala
      val task2Elements = eventCodesAndFigureUnitsTask2.collectAsList().asScala
      val task1LsEventcodes = task1Elements.map(e => e.getAs[String]("ls_event_code"))
      val task1UsageFigureUnits = task1Elements.map(e => e.getAs[String]("usage_figure_unit"))
      val task2LsEventcodes = task2Elements.map(e => e.getAs[String]("ls_event_code"))
      val task2UsageFigureUnits = task2Elements.map(e => e.getAs[String]("usage_figure_unit"))

      Then("there should be 6 rows with the following content" +
        "\n+--------+-----------+--------------+-----------------+----------+\n|customer|task_number| ls_event_code|usage_figure_unit|issue_type|\n+--------+-----------+--------------+-----------------+----------+\n|   CUST0|     TASK_1|ToT/FF+90 > FF|           TASK1A|         U|\n|   CUST0|     TASK_1|ToT/FF+90 > FF|          TASK1A2|         U|\n|   CUST0|     TASK_2|         CFUse|           TASK2A|         U|\n|   CUST0|     TASK_1|ToT/FF+90 > FF|           TASK1B|         U|\n|   CUST0|     TASK_2|         CFUse|           TASK2B|         U|\n|   CUST0|     TASK_2|         CFUse|               YE|         U|\n+--------+-----------+--------------+-----------------+----------+")
      numberOfRecords shouldBe 6
      task1LsEventcodes should contain theSameElementsAs Seq(expectedEventCodeTask1, expectedEventCodeTask1, expectedEventCodeTask1)
      task1UsageFigureUnits should contain theSameElementsAs Seq(expectedUsageFigureUnit1Task1, expectedUsageFigureUnit2Task1, expectedUsageFigureUnit3Task1)
      task2LsEventcodes should contain theSameElementsAs Seq(expectedEventCodeTask2, expectedEventCodeTask2, expectedEventCodeTask2)
      task2UsageFigureUnits should contain theSameElementsAs Seq(expectedUsageFigureUnit1Task2, expectedUsageFigureUnit2Task2, expectedUsageFigureUnit3Task2)
    }

    }