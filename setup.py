from setuptools import setup


setup(
    name='socketmap',
    desription='High-level PySpark tool for applying server-dependent functions',
    author='Mark Rogers',
    author_email='markrogersjr@gmail.com',
    url='https://github.com/markrogersjr/socketmap',
    packages=['socketmap'],
    package_dir={'': 'python'},
    install_requires=['psycopg2-binary'],
)

