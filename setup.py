# Need fastentrypoints to monkey patch setuptools for faster console_scripts
# noinspection PyUnresolvedReferences
import fastentrypoints
from setuptools import setup, find_packages
import versioneer

setup(name='firex_blaze',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='FireX event processor for submitting to a kafka bus.',
      url='https://github.com/FireXStuff/firex-blaze',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=find_packages(),
      zip_safe=True,
      install_requires=[
            'firexapp',
            'kafka-python==2.0.1',
      ],
      entry_points={
          'console_scripts': [
              'firex_blaze = firex_blaze.__main__:main',
          ],
          'firex_tracking_service':  ['firex_blaze_launcher = firex_blaze.blaze_launcher:FireXBlazeLauncher', ],
      },)
