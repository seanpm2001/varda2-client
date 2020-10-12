from setuptools import setup

setup(name='varda2-client',
    version='0.9',
    description='A python CLI to Varda2 frequency database server.',
    url='http://github.com/varda/varda2-client',
    author='Mark Santcroos',
    author_email='m.a.santcroos@lumc.nl',
    license='MIT',
    packages=['varda2_client'],
    zip_safe=False,
    entry_points = {
        'console_scripts': ['varda2-client=varda2_client:main'],
    },
    install_requires=[
        'requests'
    ],
)
