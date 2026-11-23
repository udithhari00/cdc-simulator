import random
import time
from datetime import datetime, UTC
from typing import Any, Dict, Iterable, List, Optional
from faker import Faker

from ..base import BaseSourceAdapter, CDCEvent

fake = Faker()


class MockGeneratorAdapter(BaseSourceAdapter):
    """
    Synthetic / Mock Data Generator for GlenRaven / Sterling OMS domains.
    Generates relational events across:
    1. Organizations & Customers
    2. Products & Addons
    3. Orders & Order Lines
    4. Shipments, Containers & Holds
    """

    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            fake.seed_instance(seed)
            random.seed(seed)

    def produce_events(self, scenario: str = "orders", **kwargs) -> Iterable[CDCEvent]:
        """
        Produce a sequence of CDCEvents based on the selected scenario / index name.
        Supported scenarios (matching OpenSearch index names):
        - 'orders' (alias: 'order_lifecycle'): Org -> Customer -> Order -> Lines -> Holds -> Shipment -> Invoice
        - 'products' (alias: 'catalog'): Generates N products with addons
        - 'organizations': Generates N organizations with customers and contacts
        - 'bulk_orders': Generates N orders with random lines
        """
        scenario_lower = scenario.strip().lower()

        if scenario_lower in ["orders", "order_lifecycle", "order", "order-lines"]:
            count = kwargs.get("count", 1)
            for _ in range(count):
                yield from self.generate_order_lifecycle(
                    order_no=kwargs.get("order_no"),
                    line_count=kwargs.get("line_count", 3),
                    include_shipment=kwargs.get("include_shipment", True),
                    include_hold=kwargs.get("include_hold", False),
                )
        elif scenario_lower in ["products", "catalog", "product"]:
            count = kwargs.get("count", 10)
            yield from self.generate_catalog(count=count)
        elif scenario_lower in ["organizations", "organization", "org"]:
            count = kwargs.get("count", 5)
            yield from self.generate_organizations(count=count)
        elif scenario_lower == "bulk_orders":
            count = kwargs.get("count", 10)
            yield from self.generate_bulk_orders(count=count)
        else:
            raise ValueError(f"Unknown mock scenario / index name: '{scenario}'. Valid options: 'products', 'orders', 'organizations', 'bulk_orders'")

    def generate_organizations(self, count: int = 5) -> Iterable[CDCEvent]:
        now_ts = int(datetime.now(UTC).timestamp() * 1000)

        for _ in range(count):
            org_key = f"ORG_{fake.unique.random_number(digits=8)}"
            org_code = f"ORG_CODE_{fake.unique.random_number(digits=4)}"
            person_key = f"PER_{fake.unique.random_number(digits=8)}"
            cust_key = f"CUST_{fake.unique.random_number(digits=8)}"
            contact_key = f"CON_{fake.unique.random_number(digits=8)}"

            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.email()
            phone = fake.phone_number()

            addnl_addr_key = f"ADDR_{fake.unique.random_number(digits=8)}"

            # 1. Person Info (Corporate & Contact person details)
            yield CDCEvent(
                table="YFS_PERSON_INFO",
                operation="c",
                key_field="PERSON_INFO_KEY",
                key_value=person_key,
                source_timestamp=now_ts,
                payload={
                    "PERSON_INFO_KEY": person_key,
                    "FIRST_NAME": first_name,
                    "LAST_NAME": last_name,
                    "ADDRESS_LINE1": fake.street_address(),
                    "CITY": fake.city(),
                    "STATE": fake.state_abbr(),
                    "ZIP_CODE": fake.zipcode(),
                    "COUNTRY": "US",
                    "DAY_PHONE": phone,
                    "EMAILID": email,
                    "CREATETS": datetime.now(UTC).isoformat(),
                    "MODIFYTS": datetime.now(UTC).isoformat(),
                },
            )

            # 2. Customer (Tied to buyer organization key)
            yield CDCEvent(
                table="YFS_CUSTOMER",
                operation="c",
                key_field="CUSTOMER_KEY",
                key_value=cust_key,
                source_timestamp=now_ts + 1,
                payload={
                    "CUSTOMER_KEY": cust_key,
                    "CUSTOMER_ID": f"CUST_ID_{fake.unique.random_number(digits=6)}",
                    "BUYER_ORGANIZATION_CODE": org_key,
                    "ORGANIZATION_CODE": org_code,
                    "CUSTOMER_TYPE": "01",
                    "STATUS": "ACTIVE",
                },
            )

            # 3. Customer Contact
            yield CDCEvent(
                table="YFS_CUSTOMER_CONTACT",
                operation="c",
                key_field="CUSTOMER_CONTACT_KEY",
                key_value=contact_key,
                source_timestamp=now_ts + 2,
                payload={
                    "CUSTOMER_CONTACT_KEY": contact_key,
                    "CUSTOMER_KEY": cust_key,
                    "FIRST_NAME": first_name,
                    "LAST_NAME": last_name,
                    "EMAIL_ID": email,
                    "DAY_PHONE": phone,
                },
            )

            # 4. Customer Additional Address (BillTo & ShipTo)
            yield CDCEvent(
                table="YFS_CUSTOMER_ADDNL_ADDRESS",
                operation="c",
                key_field="CUSTOMER_ADDNL_ADDRESS_KEY",
                key_value=addnl_addr_key,
                source_timestamp=now_ts + 3,
                payload={
                    "CUSTOMER_ADDNL_ADDRESS_KEY": addnl_addr_key,
                    "CUSTOMER_KEY": cust_key,
                    "PERSON_INFO_KEY": person_key,
                    "IS_BILL_TO": "Y",
                    "IS_DEFAULT_BILL_TO": "Y",
                    "IS_SHIP_TO": "Y",
                    "IS_DEFAULT_SHIP_TO": "Y",
                },
            )

            # 5. Organization (Enriches with cache from steps 1-4)
            yield CDCEvent(
                table="YFS_ORGANIZATION",
                operation="c",
                key_field="ORGANIZATION_KEY",
                key_value=org_key,
                source_timestamp=now_ts + 4,
                payload={
                    "ORGANIZATION_KEY": org_key,
                    "ORGANIZATION_CODE": org_code,
                    "ORGANIZATION_NAME": fake.company(),
                    "CORPORATE_ADDRESS_KEY": person_key,
                    "CONTACT_ADDRESS_KEY": person_key,
                    "CREATETS": datetime.now(UTC).isoformat(),
                    "MODIFYTS": datetime.now(UTC).isoformat(),
                },
            )

    def generate_catalog(self, count: int = 10) -> Iterable[CDCEvent]:
        now_ts = int(datetime.now(UTC).timestamp() * 1000)
        categories = [
            ("10604", "Fabric"),
            ("10605", "Awning Fabric"),
            ("10680", "Marine Fabric"),
            ("11551", "Upholstery Fabric"),
        ]

        fabrics = ["Sunbrella", "MarineGrade", "Coastline", "Solana", "ShadeWeave", "AquaShield"]

        for _ in range(count):
            uid = str(fake.unique.random_number(digits=7))
            cat_id, cat_name = random.choice(categories)
            base_name = f"{random.choice(fabrics)} {fake.color_name().capitalize()} Fabric"
            price = round(random.uniform(15.0, 85.0), 2)

            # 1. Product Addon
            yield CDCEvent(
                table="PRODUCT_ADDON",
                operation="c",
                key_field="uniqueID",
                key_value=uid,
                source_timestamp=now_ts,
                payload={
                    "ItemID": uid,
                    "uniqueID": uid,
                    "UnitOfMeasure": "YD",
                    "CostCurrency": "USD",
                    "DefaultProductClass": "A",
                    "Description": f"Heavy-duty outdoor grade {base_name}",
                    "Status": "3000",
                    "PrimarySupplier": "Glen Raven Custom Fabrics",
                },
            )

            # 2. Product
            yield CDCEvent(
                table="PRODUCT",
                operation="c",
                key_field="uniqueID",
                key_value=uid,
                source_timestamp=now_ts,
                payload={
                    "uniqueID": uid,
                    "partNumber": uid,
                    "name": base_name,
                    "shortDescription": f"Premium {base_name} for marine and outdoor applications.",
                    "longDescription": fake.paragraph(nb_sentences=3),
                    "category_id": cat_id,
                    "category_name": cat_name,
                    "manufacturer": "Glen Raven",
                    "price": [
                        {"currency": "USD", "value": str(price), "description": "Retail Price"}
                    ],
                    "price_USD": price,
                    "listPrice": round(price * 1.25, 2),
                    "uom": "YD",
                    "productClass": "A",
                    "inventoryStatus": "In Stock",
                    "isBuyable": "1",
                },
            )

    def generate_order_lifecycle(
        self,
        order_no: Optional[str] = None,
        line_count: int = 3,
        include_shipment: bool = True,
        include_hold: bool = False,
    ) -> Iterable[CDCEvent]:
        """
        Emits a complete synchronized order lifecycle across all tables.
        """
        now_ts = int(datetime.now(UTC).timestamp() * 1000)
        now_iso = datetime.now(UTC).isoformat()

        order_key = f"ORD_{fake.unique.random_number(digits=12)}"
        order_num = order_no or f"SO-{fake.unique.random_number(digits=6)}"
        customer_id = f"CUST_{fake.unique.random_number(digits=6)}"
        person_key = f"PER_{fake.unique.random_number(digits=8)}"

        # 1. Person info for billing & shipping
        yield CDCEvent(
            table="YFS_PERSON_INFO",
            operation="c",
            key_field="PERSON_INFO_KEY",
            key_value=person_key,
            source_timestamp=now_ts,
            payload={
                "PERSON_INFO_KEY": person_key,
                "FIRST_NAME": fake.first_name(),
                "LAST_NAME": fake.last_name(),
                "ADDRESS_LINE1": fake.street_address(),
                "CITY": fake.city(),
                "STATE": fake.state_abbr(),
                "ZIP_CODE": fake.zipcode(),
                "COUNTRY": "US",
                "DAY_PHONE": fake.phone_number(),
                "EMAILID": fake.email(),
                "CREATETS": now_iso,
                "MODIFYTS": now_iso,
            },
        )

        # 2. Order Header
        total_amount = 0.0
        order_lines = []
        for line_no in range(1, line_count + 1):
            line_key = f"{order_key}_{line_no:02d}"
            item_id = str(fake.random_number(digits=7, fix_len=True))
            unit_price = round(random.uniform(20.0, 100.0), 2)
            qty = float(random.randint(1, 10))
            line_total = round(unit_price * qty, 2)
            total_amount += line_total
            order_lines.append(
                {
                    "line_key": line_key,
                    "line_no": line_no,
                    "item_id": item_id,
                    "unit_price": unit_price,
                    "qty": qty,
                    "line_total": line_total,
                }
            )

        yield CDCEvent(
            table="YFS_ORDER_HEADER",
            operation="c",
            key_field="ORDER_HEADER_KEY",
            key_value=order_key,
            source_timestamp=now_ts,
            payload={
                "ORDER_HEADER_KEY": order_key,
                "ORDER_NO": order_num,
                "DOCUMENT_TYPE": "0001",
                "ENTERPRISE_KEY": "DEFAULT_ENT",
                "BUYER_ORGANIZATION_CODE": "DEFAULT_BUYER",
                "BILL_TO_ID": customer_id,
                "BILL_TO_KEY": person_key,
                "SHIP_TO_KEY": person_key,
                "TOTAL_AMOUNT": round(total_amount, 2),
                "ORDER_DATE": now_iso,
                "STATUS": "1100.100",  # Created
                "DRAFT_ORDER_FLAG": "N",
                "CREATETS": now_iso,
                "MODIFYTS": now_iso,
            },
        )

        # 3. Order Lines
        for line in order_lines:
            yield CDCEvent(
                table="YFS_ORDER_LINE",
                operation="c",
                key_field="ORDER_LINE_KEY",
                key_value=line["line_key"],
                source_timestamp=now_ts,
                payload={
                    "ORDER_LINE_KEY": line["line_key"],
                    "ORDER_HEADER_KEY": order_key,
                    "PRIME_LINE_NO": line["line_no"],
                    "SUB_LINE_NO": 0,
                    "LINE_TYPE": "PRODUCT",
                    "ITEM_ID": line["item_id"],
                    "ITEM_DESCRIPTION": f"Sunbrella Marine Canvas Grade Item {line['item_id']}",
                    "ORDERED_QTY": line["qty"],
                    "UNIT_PRICE": line["unit_price"],
                    "LINE_TOTAL": line["line_total"],
                    "STATUS": "1100.100",
                    "UOM": "YD",
                    "CREATETS": now_iso,
                    "MODIFYTS": now_iso,
                },
            )

        # 4. Optional Order Hold
        if include_hold:
            hold_key = f"HLD_{fake.unique.random_number(digits=8)}"
            yield CDCEvent(
                table="YFS_ORDER_HOLD_TYPE",
                operation="c",
                key_field="ORDER_HOLD_TYPE_KEY",
                key_value=hold_key,
                source_timestamp=now_ts,
                payload={
                    "ORDER_HOLD_TYPE_KEY": hold_key,
                    "ORDER_HEADER_KEY": order_key,
                    "HOLD_TYPE": "FRAUD_CHECK",
                    "STATUS": "1100",  # Active hold
                    "REASON_TEXT": "Suspected velocity check trigger",
                    "CREATETS": now_iso,
                    "MODIFYTS": now_iso,
                },
            )

        # 5. Optional Shipment & Logistics
        if include_shipment:
            shipment_key = f"SHP_{fake.unique.random_number(digits=8)}"
            container_key = f"CONT_{fake.unique.random_number(digits=8)}"
            tracking_num = f"1Z999{fake.unique.random_number(digits=12)}"

            yield CDCEvent(
                table="YFS_SHIPMENT",
                operation="c",
                key_field="SHIPMENT_KEY",
                key_value=shipment_key,
                source_timestamp=now_ts,
                payload={
                    "SHIPMENT_KEY": shipment_key,
                    "ORDER_HEADER_KEY": order_key,
                    "SHIPMENT_NO": f"SHP-{fake.unique.random_number(digits=6)}",
                    "STATUS": "1400",  # Shipped
                    "SCAC": "FEDX",
                    "CARRIER_SERVICE_CODE": "GROUND",
                    "ACTUAL_SHIPMENT_DATE": now_iso,
                    "CREATETS": now_iso,
                    "MODIFYTS": now_iso,
                },
            )

            yield CDCEvent(
                table="YFS_SHIPMENT_CONTAINER",
                operation="c",
                key_field="SHIPMENT_CONTAINER_KEY",
                key_value=container_key,
                source_timestamp=now_ts,
                payload={
                    "SHIPMENT_CONTAINER_KEY": container_key,
                    "SHIPMENT_KEY": shipment_key,
                    "TRACKING_NO": tracking_num,
                    "CONTAINER_NO": f"C_{fake.unique.random_number(digits=4)}",
                    "CREATETS": now_iso,
                    "MODIFYTS": now_iso,
                },
            )

            for line in order_lines:
                ship_line_key = f"SHPLINE_{fake.unique.random_number(digits=8)}"
                details_key = f"CDET_{fake.unique.random_number(digits=8)}"

                yield CDCEvent(
                    table="YFS_SHIPMENT_LINE",
                    operation="c",
                    key_field="SHIPMENT_LINE_KEY",
                    key_value=ship_line_key,
                    source_timestamp=now_ts,
                    payload={
                        "SHIPMENT_LINE_KEY": ship_line_key,
                        "SHIPMENT_KEY": shipment_key,
                        "ORDER_LINE_KEY": line["line_key"],
                        "ORDER_HEADER_KEY": order_key,
                        "QUANTITY": line["qty"],
                        "CREATETS": now_iso,
                        "MODIFYTS": now_iso,
                    },
                )

                yield CDCEvent(
                    table="YFS_CONTAINER_DETAILS",
                    operation="c",
                    key_field="CONTAINER_DETAILS_KEY",
                    key_value=details_key,
                    source_timestamp=now_ts,
                    payload={
                        "CONTAINER_DETAILS_KEY": details_key,
                        "SHIPMENT_CONTAINER_KEY": container_key,
                        "SHIPMENT_LINE_KEY": ship_line_key,
                        "QUANTITY": line["qty"],
                        "CREATETS": now_iso,
                        "MODIFYTS": now_iso,
                    },
                )

        # 6. Order Invoiced
        invoice_key = f"INV_{fake.unique.random_number(digits=8)}"
        yield CDCEvent(
            table="YFS_ORDER_INVOICE",
            operation="c",
            key_field="ORDER_INVOICE_KEY",
            key_value=invoice_key,
            source_timestamp=now_ts,
            payload={
                "ORDER_INVOICE_KEY": invoice_key,
                "ORDER_HEADER_KEY": order_key,
                "INVOICE_NO": f"INV-{fake.unique.random_number(digits=6)}",
                "TOTAL_AMOUNT": round(total_amount, 2),
                "DATE_INVOICED": now_iso,
                "CREATETS": now_iso,
                "MODIFYTS": now_iso,
            },
        )

    def generate_bulk_orders(self, count: int = 10) -> Iterable[CDCEvent]:
        for i in range(count):
            yield from self.generate_order_lifecycle(
                line_count=random.randint(1, 4),
                include_shipment=(random.random() > 0.3),
                include_hold=(random.random() > 0.8),
            )

