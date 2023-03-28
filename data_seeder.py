
class DataSeeder:
    """
    This class is used to seed the database with data
    """

    def __init__(self, number_of_records: int = 10, exclude_list: list = None) -> None:
        self.number_of_records = number_of_records
        engine = create_engine(f"postgresql+psycopg2://{Config.postgres_connection}")
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.fake = Faker()
        self.metadata = MetaData(bind=engine)
        self.metadata.reflect()
        self.mapped = {}
        # exclude models from generation
        self.exclude_list = exclude_list

    @staticmethod
    def snake_to_pascal_case(name: str) -> str:
        """
        It takes a string in snake case and returns a string in Pascal case

        :param name: The name of the class to be generated
        :type name: str
        :return: A string with the first letter of each word capitalized.
        """
        return "".join(word.capitalize() for word in name.split("_"))

    @staticmethod
    def save_model(model, row_data) -> Optional[bool]:
        """
        It takes a model and a dictionary of data, and saves the data to the database if it does not violate any constraints

        :param model: The model to save the data to
        :param row_data: A dictionary of the row data
        """
        try:
            model.get_or_create(model, **row_data)
            # commit changes to the database
            model.session.commit()
        except IntegrityError:
            # handle unique constraint violation by rolling back the transaction
            model.session.rollback()

    def get_model_class(self, table_name: str) -> ModelType:
        """
        It imports the module `app.api.{route}.models`
        and returns the class `{table_name}` from that module

        :param table_name: The name of the table you want to get the model class for
        :type table_name: str
        :return: The model class for the table name.
        """

        for route in APIPrefix.include:
            with contextlib.suppress(ImportError, AttributeError):
                module = __import__(f"app.api.{route}.models", fromlist=[table_name])
                class_name = DataSeeder.snake_to_pascal_case(table_name)
                return getattr(module, class_name)


    def get_model_metadata(self):
        # import metadata for all routes to build the registry
        for route in APIPrefix.include:
            with contextlib.suppress(ImportError):
                if route != "auth":
                    exec(f"from app.api.{route}.models import ModelMixin as Base")

        # loop through models in registry
        for table in sorted(Base.metadata.sorted_tables, key=lambda t: t.name, reverse=True):
            if table.name not in self.metadata.tables: #or table.name in self.exclude_list:
                continue

            if model := self.get_model_class(table.name):
                model.name = model.__name__
                yield model, table

    def get_data_type_mapper(self) -> SimpleNamespace:
        """
        returns a list of objects that have a type and fake_type attribute

        :param table: The table name
        :return: A list of objects with the type and fake_type attributes.
        """
        return SimpleNamespace(
            type_maps=[
                 SimpleNamespace(
                    type=DateTime,
                    fake_type=self.fake.date_time_between(
                        start_date="-30y", end_date="now"
                    ),
                ),
                SimpleNamespace(type=Boolean, fake_type=self.fake.boolean()),
                SimpleNamespace(type=Integer, fake_type=self.fake.random_int()),
                SimpleNamespace(type=Float, fake_type=self.fake.pyfloat(positive=True)),
                SimpleNamespace(
                    type=Interval, fake_type=timedelta(seconds=randint(0, 86400))
                ),
                SimpleNamespace(type=UUID, fake_type=str(uuid.uuid4())),
                SimpleNamespace(
                    type=String,
                    fake_type=f"{' '.join([self.fake.word() for _ in range(8)])}",
                ),
            ]
        )


    def get_table_data(self, table, column) -> List[ModelType]:
        """
        It returns a list of all the values in a given column of a given table

        :param table: The name of the table you want to query
        :param column: The column name to get data from
        :return: A list of all the values in the column of the table.
        """
        return self.session.query(getattr(self.get_model_class(table), column)).all()


    def generate_fake_row_data(self, table: MetaData) -> dict:
        """
        Loop through all table columns, check data types for all columns in a
        table and generate fake data, ensure pk is unique, loop through table
        relationships, link fk to existing record, or make one first then build relationship

        :param table: The table object that we're generating data for
        :return: A dictionary of column names and fake data.
        """

        # loop through all table columns
        row_data = {}
        for column in table.columns:
            # Check data types for all columns in a table and generate fake data
            data_type_mapper = self.get_data_type_mapper()
            for data_type in data_type_mapper.type_maps:
                if isinstance(column.type, data_type.type):
                    row_data[column.name] = data_type.fake_type

            # ensure pk is unique
            if column.primary_key and isinstance(column.type, UUID):
                row_data[column.name] = str(uuid.uuid4())
            if column.primary_key and isinstance(column.type, int):
                row_data[column.name] = self.fake.random_int() * self.fake.random_int()

            # loop through table relationships
            for fk in column.foreign_keys:
                fk_table = fk.column.table
                fk_column = fk.column

                # link fk to existing record, or make one first then build relationship
                if fk_records := self.get_table_data(fk_table.name, fk_column.name):
                    row_data[column.name] = choice(fk_records)[0]
                else:
                    new_data = self.generate_fake_row_data(fk_table)
                    self.save_model(self.get_model_class(fk_table.name), new_data)

        return row_data


    def generate(self):
        """
        For each model in the registry, generate a number 
        of fake records equal to the number of records specified by 
        the user, and save them to the database.
        """
        
        # loop through models in registry
        for model, table in list(self.get_model_metadata()):

            for _ in range(self.number_of_records):
                row_data = self.generate_fake_row_data(table)
                self.save_model(model, row_data)
            print(model, f"{self.number_of_records} records added to db")
