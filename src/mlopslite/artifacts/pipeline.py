from sklearn.pipeline import Pipeline


class ModelPipe(Pipeline):

    """
    Extends the Pipeline class in scikit-learn
    pass to and from DB
    """

    @staticmethod
    def bind_to_dataset(self, id: int) -> None:
        pass

    def extract_input_columns(self):
        pass

    # bind Pipeline to DataSet
    # extract only used raw column references
    # extract pipeline definition / model definition
    # write Pipeline as bytes to DB
