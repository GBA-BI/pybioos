from setuptools import find_packages, setup

from bioos.__about__ import __version__

setup(
    name="pybioos",
    version=__version__,
    keywords=["pip", "bioos"],
    description="BioOS SDK for Python",
    license="MIT Licence",
    url="https://github.com/GBA-BI/pybioos",
    author="Jilong Liu",
    author_email="liu_jilong@gzlab.ac.cn",
    packages=find_packages(),
    platforms="any",
    install_requires=[
        "volcengine>=1.0.61", "tabulate>=0.8.10", "click>=8.0.0",
        "pandas>=1.3.0", "tos==2.5.6", "cachetools>=5.2.0",
        "typing-extensions>=4.4.0", "apscheduler>=3.10.4", "colorama>=0.4.6"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator"
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'bioos_workflow_submit=bioos.bioos_workflow:bioos_workflow',
            'bioos_workflow_import=bioos.bw_import:bioos_workflow_import',
            'bioos_workflow_import_status=bioos.bw_import_status_check:bioos_workflow_status_check',
            'bioos_workflow_run_status=bioos.bw_status_check:bioos_workflow_status_check',
            'bioos_submission_logs=bioos.get_submission_logs:get_submission_logs',
            'bioos_workspace_list=bioos.cli.list_bioos_workspaces:main',
            'bioos_workspace_workflow_list=bioos.cli.list_workflows_from_workspace:main',
            'bioos_workspace_submission_list=bioos.cli.list_submissions_from_workspace:main',
            'bioos_workflow_inputs_template=bioos.cli.generate_inputs_json_template_bioos:main',
            'bioos_workspace_create=bioos.cli.create_workspace_bioos:main',
            'bioos_workspace_file_list=bioos.cli.list_files_from_workspace:main',
            'bioos_workspace_file_download=bioos.cli.download_files_from_workspace:main',
            'bioos_ies_create=bioos.cli.create_iesapp:main',
            'bioos_ies_status=bioos.cli.check_ies_status:main',
            'bioos_ies_events=bioos.cli.get_ies_events:main',
            'bioos_workspace_profile=bioos.cli.get_workspace_profile:main',
            'bioos_wdl_validate=bioos.cli.validate_wdl:main',
            'bioos_submission_delete=bioos.cli.delete_submission:main',
            'bioos_workspace_export=bioos.cli.export_bioos_workspace:main',
            'bioos_workspace_dashboard_upload=bioos.cli.upload_dashboard_file:main',
            'bioos_dockstore_search=bioos.cli.search_dockstore:main',
            'bioos_dockstore_wdl_fetch=bioos.cli.fetch_wdl_from_dockstore:main',
            'bioos_docker_image_url=bioos.cli.get_docker_image_url:main',
            'bioos_docker_build=bioos.cli.build_docker_image:main',
            'bioos_docker_build_status=bioos.cli.check_build_status:main'
        ]
    })
