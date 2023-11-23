'''
Dengan menyimpan variables ke dalam sistem airflow:
admin > variables 

kemudian isi column yang ingin ditentukan

seperti code dibawah ini:

from airflow.models import Variable

def get_var_func():

    # Auto-deserializes a JSON value
    book_entities_var = Variable.get("book_entities", deserialize_json=True)

    # Returns the value of default_var (None) if the variable is not set
    program_name_var = Variable.get("program_name")

    print(f'Print variables, program_name {program_name_var}')
    print(f'Print variables, book_entities  {book_entities_var}')


'''