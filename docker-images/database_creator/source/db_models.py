from sqlalchemy import (Boolean, Column, ForeignKeyConstraint, Integer,
                        MetaData, Sequence, String, Table)


def create_tables(schema_name):
    try:
        metadata = MetaData(schema=schema_name)

        Table(
            'property_area',
            metadata,
            Column(
                'id',
                Integer,
                Sequence('property_id_seq', start=1, increment=1),
                nullable=False,
                primary_key=True,
            ),
            Column('property_area', String(10), nullable=False),
        )

        Table(
            'customers',
            metadata,
            Column(
                'id',
                Integer,
                nullable=False,
                primary_key=True,
                autoincrement=True,
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
            Column('customer_id', Integer, nullable=False),
            ForeignKeyConstraint(
                ['customer_id'],
                ['customers.id'],
                name='fk_customer_id',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
            Column('property_area_id', Integer, nullable=False),
            ForeignKeyConstraint(
                ['property_area_id'],
                ['property_area.id'],
                name='fk_property_area',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
            Column('prediction_id', Integer, nullable=True),
            ForeignKeyConstraint(
                ['prediction_id'],
                ['loan_status_prediction.id'],
                name='fk_prediction_results',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
        )

        Table(
            'loan_status_prediction',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('loan_status', String(1), nullable=True),
        )

        return metadata
    except Exception as e:
        logging.error(e)
