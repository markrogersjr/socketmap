from setuptools import setup


setup(
    name='socketmap',
    description='High-level PySpark tool for applying server-dependent functions',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    version='0.2.5',
    author='Mark Rogers',
    author_email='m@inmimo.me',
    url='https://github.com/markrogersjr/socketmap',
    packages=['socketmap'],
    package_dir={'': 'python'},
    install_requires=['psycopg2-binary'],
)

