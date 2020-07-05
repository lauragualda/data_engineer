import setuptools

setuptools.setup(
    name="meal_db",
    version="1.0",
    description="A python client to connect to The Meal DB public API.",
    author="Laura Gualda",
    author_email="lgualda@gmail.com",
    packages=setuptools.find_packages(),
    install_requires=[
        'pandas==1.0.4',
        'requests==2.23.0',
        'responses==0.10.14'
    ],
    scripts=['meal_db_client.py']
)
