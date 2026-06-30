#!/usr/bin/env python3

"""
Generic Metamist analysis generation script, must be executable

- takes a path to a primary output, and optionally one or more secondary analyses
  - due to the fixed dictionary structure (type of each secondary is within schema), this requires 'k=v k2=v2' args.
- requires an analysis type to register this as
- can take either SGs or Cohorts to associate the analysis entry with, but not both
- optionally can use the same k=v breaking to register metadata

e.g. CLI

# minimal example
python3 -m cpg_utils.metamist_registration \
    --project <project> \
    --output <path/to/primary/output> \
    --type cram \
    --cohorts COH1234 \
    --dry

# with optional metaadta
python3 -m cpg_utils.metamist_registration \
    --project <project> \
    --output <path/to/primary/output> \
    --type cram \
    --secondary index=path/to/index html=/path/to/html \
    --meta stage=name sequencing_type=genome \
    --cohorts COH1234 COH5678

# imported equivalent using a method call
from cpg_utils.metamist_registration import create_new
create_new(
    project=<project>,
    output=<path/to/primary/output>,
    analysis_type=cram,
    meta={'stage': 'name', 'sequencing_type': 'genome'},
    cohorts=['COH1234', 'COH5678'],
)

This will produce analysis records where:
 - the analysis record is of a primary type defined by --type
 - the analysis record has an 'outputs' dictionary
 - outputs.path is the value of --output
 - if used, secondary files will be nested inside `analysis.outputs.secondary_files.TYPE.path`
 - if used, analysis.meta will contain all the meta key=value pairs
"""

import argparse  # noqa: I001
import json
import sys

from cpg_utils.existence_checks import exists
from metamist import exceptions, graphql


GS_PREFIX = 'gs://'
UPDATE_QUERY = """
    mutation updateAnalysis($project: String!, $analysis:AnalysisInput!) {
        analysis {
            createAnalysis(project:$project, analysis:$analysis) {
                id
                type
                status
                outputs
                active
            }
        }
    }
    """


def parse_cli_kv(input_kv: list[str]) -> dict[str, str]:
    """
    Takes a list of key-value pairs and parses them into a dictionary.
    Used in populating both meta and secondary files.

    Args:
        input_kv: list[str] a list of key-value pairs passed form the CLI

    Returns:
        a dictionary of key-value pairs built from the original strings
    """
    broken_kv = {}
    for keyvaluepair in input_kv:
        if '=' not in keyvaluepair:
            raise ValueError(f'Found key=value entry which lacks a "=": {keyvaluepair}')

        key, value = keyvaluepair.split('=')
        if key in broken_kv:
            raise ValueError(f'Duplicate key provided: {key}')
        broken_kv[key] = value
    return broken_kv


def find_missing_files(primary: str, secondary: dict[str, str]) -> set[str]:
    """For the primary and secondary files, detect if any are missing. Return all missing Paths."""
    missing_files: set[str] = set()
    for filepath in [primary, *list(secondary.values())]:
        if not exists(filepath):
            missing_files.add(filepath)
    return missing_files


def find_non_gcs_files(primary: str, secondary: dict[str, str]) -> set[str]:
    """Checks for a `gs://` prefix on all files."""
    non_gs_files: set[str] = set()
    for filepath in [primary, *list(secondary.values())]:
        if not filepath.startswith('gs://'):
            non_gs_files.add(filepath)
    return non_gs_files


def create_output_block(
    primary: str,
    secondary: dict[str, str],
) -> dict:
    """Populates the output dict based on the provided arguments."""

    # create the outputs dictionary, with the primary file's full path at the root
    outputs: dict = {'basename': primary}

    # take the dict of type: file secondary files, and add to the analysis
    if secondary:
        outputs['secondary_files'] = {
            key: {'basename': value} for key, value in secondary.items()
        }

    return outputs


def cli_main():
    parser = argparse.ArgumentParser(description='Record MultiQC results to Metamist.')
    parser.add_argument(
        '--project',
        required=True,
        help='The Metamist project name.',
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Path of the primary output file/dir.',
    )
    parser.add_argument(
        '--type',
        required=True,
        help='Analysis type, must be from valid enum.',
    )
    parser.add_argument(
        '--secondary',
        nargs='+',
        help='Optional, list of k=v pairs for secondary analyses.',
        default=[],
    )
    parser.add_argument(
        '--meta',
        nargs='+',
        help='List of k=v metadata pairs, optional.',
        default=[],
    )
    parser.add_argument(
        '--cohorts',
        nargs='+',
        help='Metamist cohort ID(s).',
        default=[],
    )
    parser.add_argument(
        '--sgs',
        nargs='+',
        help='Metamist Sequencing Group ID(s).',
        default=[],
    )
    parser.add_argument(
        '--dry',
        action='store_true',
        help='Dry run, print only.',
    )
    args = parser.parse_args()

    # do some transformation of the input meta/secondary file dictionaries
    meta_dict = parse_cli_kv(args.meta)
    secondary_dict = parse_cli_kv(args.secondary)

    # call the main analysis generation method
    create_new(
        project=args.project,
        output=args.output,
        analysis_type=args.type,
        cohorts=args.cohorts,
        sgs=args.sgs,
        dry=args.dry,
        meta=meta_dict,
        secondary=secondary_dict,
    )


def create_new(
    project: str,
    output: str,
    analysis_type: str,
    meta: dict[str, str] | None = None,
    secondary: dict[str, str] | None = None,
    cohorts: list[str] | None = None,
    sgs: list[str] | None = None,
    dry: bool = False,
) -> None:
    """
    main method, takes the provided inputs and creates a new analysis entry

    Args:
        project: str, the name of the project to create the analysis entry in
        output: str, the primary output path of the analysis entry
        analysis_type: str, the analysis type (must be from valid enum in metamist)
        meta: optional dict, if provided this will be added as the analysis.meta dictionary
        secondary: optional dict, if provided each element of this {type: string} dict will be added as a secondary file
        cohorts: optional list, COHort IDs to attribute the analysis to. Mutually exclusive with sgs
        sgs: optional list, Sequencing Group IDs to attribute the analysis to. Mutually exclusive with cohorts
        dry: bool, if True, payload is printed, but not sent
    """
    # fail if the cohorts and sgs are both applied, or if neither is applied
    if cohorts and sgs:
        raise ValueError(
            'Cannot specify both --cohorts and --sgs CLI parameters for a single Analysis.',
        )

    if not (cohorts or sgs):
        raise ValueError(
            'You must specify either --cohorts or --sgs for a single Analysis object.',
        )

    if missing_files := find_missing_files(primary=output, secondary=secondary or {}):
        missing_file_string = ', '.join(sorted(missing_files))
        raise ValueError(
            f'Missing files detected: {missing_file_string}.\nThis can only be used for extant files.',
        )
    if non_gcs_files := find_non_gcs_files(primary=output, secondary=secondary or {}):
        missing_file_string = ', '.join(sorted(non_gcs_files))
        raise ValueError(
            f'Non-GCS files detected: {missing_file_string}.\nThis can only be used for files stored in GCS.',
        )

    outputs = create_output_block(
        primary=output,
        secondary=secondary or {},
    )

    variables: dict = {
        'project': project,
        'analysis': {
            'type': analysis_type,
            'status': 'COMPLETED',
            'outputs': outputs,
            'meta': meta,
            'cohortIds': cohorts or None,
            'sequencingGroupIds': sgs or None,
        },
    }

    if dry:
        print('DRY RUN, would have POSTed the following analysis data:')
        print(json.dumps(variables, indent=2))
    else:
        try:
            result = graphql.query(UPDATE_QUERY, variables)
            print(f'Successfully recorded analysis to Metamist: {result}')
        except exceptions.ApiException as e:
            print(f'Error executing GraphQL query: {e}', file=sys.stderr)
            raise e


if __name__ == '__main__':
    cli_main()
