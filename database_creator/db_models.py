from sqlalchemy import Boolean, Column, ForeignKeyConstraint, Integer, MetaData, Sequence, String, Table

def create_tables(schema_name, train=True): 
    try:
        metadata = MetaData(schema=schema_name)

        print('0')

        x = Table(
            "property_area",
            metadata,
            Column('id', Integer, nullable=False, primary_key=True),
            Column('area_name', String(10), nullable=False),
        )

        print('1')
        b = Table(
            "customers",
            metadata,
            Column('id', Integer, Sequence('customers_id_seq', start=1, increment=1), nullable=False, primary_key=True, autoincrement=True),
            Column('gender', String(6), nullable=True),
            Column('married', Boolean, nullable=True),
            Column('dependents', String(3), nullable=True),
            Column('self_employed', Boolean, nullable=True),
            Column('applicant_income', Integer, nullable=True),
            Column('coapplicant_income', Integer, nullable=True),
            Column('credit_history', Boolean, nullable=False),
        )
        print('2')

        c = Table(
            "loan_details",
            metadata,
            Column('id', Integer, Sequence('details_seq', start=1, increment=1), nullable=False, primary_key=True, autoincrement=True),
            Column('loan_details', Integer, nullable=False),
            Column('amount_term', Integer, nullable=False),
        )
        print('3')

        d = Table(
            "loans",
            metadata,
            Column('id', String(8), nullable=False, primary_key=True),
            Column('loan_details', Integer, nullable=False),
            ForeignKeyConstraint(
                ['loan_details'],
                ['loan_details.id'],
                name='fk_loan_details',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
            Column('customerId', Integer, nullable=False),
            ForeignKeyConstraint(
                ['customerId'],
                ['customers.id'],
                name='fk_customer_id',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
            Column('property_area', Integer, nullable=False),
            ForeignKeyConstraint(
                ['property_area'],
                ['property_area.id'],
                name='fk_property_area',
                onupdate='CASCADE',
                ondelete='SET NULL',
            ),
        )
        print('here')

        return metadata
    except Exception as e:
        print(e)