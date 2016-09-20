from setuptools import setup, find_packages

print find_packages()
setup(
        name="walker",
        version="0.1",
        packages = find_packages(),
        #install_requires = ['boto3', 'ansible', 'requests','google-api-python-client']
)

