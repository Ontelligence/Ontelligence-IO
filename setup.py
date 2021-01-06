from setuptools import setup, find_packages


def readme():
    with open('README.md', 'r') as f:
        return f.read()


setup(
    name='ontelligence',
    version='0.1.0',
    description='',
    long_description=readme(),
    long_description_content_type='text/markdown',
    classifiers=[],
    url='https://github.com/hamzaahmad-io/Ontelligence-IO',
    author='Hamza Ahmad',
    author_email='hamza.ahmad@me.com',
    keywords='python ontelligence io data management transformation',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'cached-property~=1.5',
        'pandas>=0.17.1, <2.0',
        'pydantic==1.6.1',
        'dacite==1.5.1',
        'sqlparse==0.4.1',
        'pysftp==0.2.9',
        'boto3',
        'cchardet==2.1.7',
        'pendulum==2.1.2',
        'snowflake-connector-python',
        'flake8',
        'smart_open[all]'
    ],
    include_package_data=True,
    zip_safe=False
)
