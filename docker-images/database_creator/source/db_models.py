import logging
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    Sequence,
    String,
    Table,
)


def create_tables(schema_name):
    try:
        metadata = MetaData(schema=schema_name)

        Table(
            'property_area',
            metadata,
            Column(
                'property_area_id',
                String(255),
                primary_key=True,
            ),
            Column('property_area', String(10), nullable=False),
        )

        Table(
            'customers',
            metadata,
            Column(
                'customer_id',
                String(255),
                nullable=False,
                primary_key=True,
            ),
            Column('gender', String(6), nullable=True),
            Column('married', Boolean, nullable=True),
            Column('graduated', Boolean, nullable=True),
            Column('dependents', String(3), nullable=True),
            Column('self_employed', Boolean, nullable=True),
            Column('applicant_income', Integer, nullable=True),
            Column('coapplicant_income', Integer, nullable=True),
            Column('credit_history', Boolean, nullable=True),
        )

        Table(
            'loans',
            metadata,
            Column('loan_id', String(8), nullable=False, primary_key=True),
            Column('loan_amount', Integer, nullable=True),
            Column('loan_amount_term', Integer, nullable=True),
            Column('customer_id', String(255), nullable=False),
            ForeignKeyConstraint(
                ['customer_id'],
                ['customers.customer_id'],
                name='fk_customer_id',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
            Column('property_area_id', String(255), nullable=False),
            ForeignKeyConstraint(
                ['property_area_id'],
                ['property_area.property_area_id'],
                name='fk_property_area',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
            Column('prediction_id', String(255), nullable=True),
            ForeignKeyConstraint(
                ['prediction_id'],
                ['loan_status_prediction.prediction_id'],
                name='fk_prediction_results',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
        )

        Table(
            'loan_status_prediction',
            metadata,
            Column('prediction_id', String(255), primary_key=True),
            Column('loan_status', String(1), nullable=True),
        )

        return metadata
    except Exception as e:
        logging.error(e)
