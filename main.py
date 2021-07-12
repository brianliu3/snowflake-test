import os
import snowflake.connector
from dotenv import load_dotenv
load_dotenv()


def main():
    ctx = snowflake.connector.connect(
        user=os.environ['USERNAME'],
        password=os.environ['PASSWORD'],
        account=os.environ['ACCOUNT'],
    )
    cs = ctx.cursor()
    try:
        cs.execute("use role analyst")
        cs.execute("use database fivetran_db")
        cs.execute("use warehouse analyst_warehouse")
        cs.execute("""
            select
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."ID" as "OPPORTUNITY.ID",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."ACCOUNT_ID" as "OPPORTUNITY.ACCOUNT_ID",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."ID" as "OPPORTUNITY_LINE_ITEM.ID",
            "FIVETRAN_DB"."SALESFORCE"."ACCOUNT"."PARENT_ID" as "ACCOUNT.PARENT_ID",
            "FIVETRAN_DB"."SALESFORCE"."ACCOUNT"."NAME" as "ACCOUNT.NAME",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."NAME" as "OPPORTUNITY.NAME",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."LICENSE_TERM_START_C" as "OPPORTUNITY.LICENSE_TERM_START_C",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."LICENSE_TERM_END_C" as "OPPORTUNITY.LICENSE_TERM_END_C",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."SUPPORT_TYPE_C" as "OPPORTUNITY.SUPPORT_TYPE_C",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."PRICING_MODEL_C" as "OPPORTUNITY_LINE_ITEM.PRICING_MODEL_C",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."PRODUCT_CODE" as "OPPORTUNITY_LINE_ITEM.PRODUCT_CODE",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."UNIQUE_NAME_C" as "OPPORTUNITY_LINE_ITEM.UNIQUE_NAME_C",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."QUANTITY" as "OPPORTUNITY_LINE_ITEM.QUANTITY",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."QUANTITY_C" as "OPPORTUNITY_LINE_ITEM.QUANTITY_C",
            "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."PRODUCT_2_ID" as "OPPORTUNITY_LINE_ITEM.PRODUCT_2_ID",
            "FIVETRAN_DB"."SALESFORCE"."PRODUCT_2"."DESCRIPTION" as "PRODUCT_2.DESCRIPTION"
            from "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"
            inner join "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"
            on "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."ID" = "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."OPPORTUNITY_ID"
            inner join "FIVETRAN_DB"."SALESFORCE"."PRODUCT_2"
            on "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"."PRODUCT_2_ID" = "FIVETRAN_DB"."SALESFORCE"."PRODUCT_2"."ID"
            inner join "FIVETRAN_DB"."SALESFORCE"."ACCOUNT"
            on "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."ACCOUNT_ID" = "FIVETRAN_DB"."SALESFORCE"."ACCOUNT"."ID"
            where "FIVETRAN_DB"."SALESFORCE"."OPPORTUNITY"."IS_WON"=TRUE;
        """)
        one_row = cs.fetchone()
        print(one_row)
    finally:
        cs.close()
    ctx.close()


if __name__ == '__main__':
    main()
