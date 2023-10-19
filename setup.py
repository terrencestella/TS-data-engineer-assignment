from setuptools import setup,find_packages

if __name__ == "__main__":
    setup(
        name='TS_exercise_data_pipeline',
        version='0.9.8',
        package_dir={'':'src'},
        packages=find_packages(where='src'),
        description='This package is the result of an exercise given to author',
        author='Terrence Stella',
        author_email='terrence.s.stella@gmail.com',
        install_requires=['pyspark','argparse','chispa'],
        classifiers=[
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3.8',
        ],
        keywords='KommatiPara',
        url='https://github.com/terrencestella/TS-data-engineer-assignment',
    )