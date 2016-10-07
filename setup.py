from setuptools import setup, find_packages

print find_packages()
setup(
        name="walker",
        version="0.1",
        packages = find_packages(),
        install_requires = ['elasticsearch', 'kafka-python', 'tornado', 'flask', 'hdfs', 'snakebite']
)

