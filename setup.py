from setuptools import setup

setup(
    name='etl_oms',
    version='0.1.0',
    py_modules=['ETL_OMS_OPERATIONNEL'],
    install_requires=[
        'pandas',
        'psycopg2',
    ],
    entry_points={
        'console_scripts': [
            'etl_oms=ETL_OMS_OPERATIONNEL:main',  # à adapter selon ta fonction principale
        ],
    },
)
