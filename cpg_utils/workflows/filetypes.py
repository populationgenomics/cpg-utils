"""
Wrappers for bioinformatics file types (CRAM, GVCF, FASTQ, etc).
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union

from hailtop.batch import ResourceGroup, ResourceFile, Batch

from cpg_utils import Path, to_path
from cpg_utils.workflows.utils import exists


class AlignmentInput(ABC):
    """
    Data that works as input for alignment or realignment.
    """

    @abstractmethod
    def exists(self) -> bool:
        """
        Check if all files exist.
        """


class CramOrBamPath(AlignmentInput, ABC):
    """
    Represents a path to a CRAM or a BAM file, optionally with corresponding index.
    """

    def __init__(
        self,
        path: str | Path,
        index_path: str | Path | None = None,
        reference_assembly: str = None,
    ):
        self.path = to_path(path)
        self.index_path: Path | None = None
        self.full_index_suffix: str | None = None
        if index_path:
            self.index_path = to_path(index_path)
            assert self.index_path.suffix == f'.{self.index_ext}'
            self.full_index_suffix = str(self.index_path).replace(
                str(self.path.with_suffix('')), ''
            )

    @property
    @abstractmethod
    def ext(self) -> str:
        ...

    @property
    @abstractmethod
    def index_ext(self) -> str:
        ...

    def __str__(self) -> str:
        """
        >>> str(CramPath('gs://bucket/sample.cram', 'gs://bucket/sample.cram.crai'))
        'CRAM(gs://bucket/sample{.cram,.cram.crai})'
        """
        res = str(self.path)
        if self.index_path:
            assert self.full_index_suffix
            res = (
                str(self.path.with_suffix(''))
                + f'{{{self.path.suffix},{self.full_index_suffix}}}'
            )
        return f'{self.ext.upper()}({res})'

    def exists(self) -> bool:
        """
        CRAM file exists.
        """
        return exists(self.path)

    def resource_group(self, b: Batch) -> ResourceGroup:
        """
        Create a Hail Batch resource group
        """
        d = {
            self.ext: str(self.path),
        }
        if self.full_index_suffix:
            d[self.full_index_suffix] = str(self.index_path)

        return b.read_input_group(**d)


class BamPath(CramOrBamPath):
    """
    Represents a path to a BAM file, optionally with corresponding index.
    """

    EXT = 'bam'
    INDEX_EXT = 'bai'

    def __init__(
        self,
        path: str | Path,
        index_path: str | Path | None = None,
    ):
        super().__init__(path, index_path)

    @property
    def ext(self) -> str:
        return BamPath.EXT

    @property
    def index_ext(self) -> str:
        return BamPath.INDEX_EXT


class CramPath(CramOrBamPath):
    """
    Represents a path to a CRAM file, optionally with corresponding index.
    """

    EXT = 'cram'
    INDEX_EXT = 'crai'

    def __init__(
        self,
        path: str | Path,
        index_path: str | Path | None = None,
        reference_assembly: str | Path = None,
    ):
        self.reference_assembly = None
        if reference_assembly:
            self.reference_assembly = to_path(reference_assembly)
        super().__init__(path, index_path)
        self.somalier_path = to_path(f'{self.path}.somalier')

    @property
    def ext(self) -> str:
        return CramPath.EXT

    @property
    def index_ext(self) -> str:
        return CramPath.INDEX_EXT


class GvcfPath:
    """
    Represents GVCF data on a bucket within the workflow.
    Includes a path to a GVCF file along with a corresponding TBI index,
    and a corresponding fingerprint path.
    """

    def __init__(self, path: Path | str):
        self.path = to_path(path)
        self.somalier_path = to_path(f'{self.path}.somalier')

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        return f'GVCF({self.path})'

    def exists(self) -> bool:
        """
        GVCF file exists.
        """
        return self.path.exists()

    @property
    def tbi_path(self) -> Path:
        """
        Path to the corresponding index
        """
        return to_path(f'{self.path}.tbi')

    def resource_group(self, b: Batch) -> ResourceGroup:
        """
        Create a Hail Batch resource group
        """
        return b.read_input_group(
            **{
                'g.vcf.gz': str(self.path),
                'g.vcf.gz.tbi': str(self.tbi_path),
            }
        )


FastqPath = Union[str, Path, ResourceFile]


@dataclass
class FastqPair:
    """
    Pair of FASTQ files
    """

    r1: FastqPath
    r2: FastqPath

    def __getitem__(self, i):
        assert i == 0 or i == 1, i
        return [self.r1, self.r2][i]

    def as_resources(self, b) -> 'FastqPair':
        """
        Makes a pair of ResourceFile objects for r1 and r2.
        """
        return FastqPair(
            *[
                self[i]
                if isinstance(self[i], ResourceFile)
                else b.read_input(str(self[i]))
                for i in [0, 1]
            ]
        )

    def __str__(self):
        return f'{self.r1}|{self.r2}'


class FastqPairs(list[FastqPair], AlignmentInput):
    """
    Multiple FASTQ file pairs belonging to the same sample
    (e.g. multiple lanes or top-ups).
    """

    def exists(self) -> bool:
        """
        Check if each FASTQ file in each pair exist.
        """
        return all(exists(pair.r1) and exists(pair.r2) for pair in self)

    def __str__(self) -> str:
        """
        Glob string to find all FASTQ files.

        >>> str(FastqPairs([
        >>>     FastqPair('gs://sample_R1.fq.gz', 'gs://sample_R2.fq.gz'),
        >>> ]))
        'gs://sample_R{2,1}.fq.gz'
        >>> str(FastqPairs([
        >>>     FastqPair('gs://sample_L1_R1.fq.gz', 'gs://sample_L1_R2.fq.gz'),
        >>>     FastqPair('gs://sample_L2_R1.fq.gz', 'gs://sample_L2_R2.fq.gz'),
        >>> ]))
        'gs://sample_L{2,1}_R{2,1}.fq.gz'
        """
        all_fastq_paths = []
        for pair in self:
            all_fastq_paths.extend([pair.r1, pair.r2])
        # Triple braces are intentional: they are resolved to single ones.
        return ''.join(
            [
                f'{{{",".join(set(chars))}}}' if len(set(chars)) > 1 else chars[0]
                for chars in zip(*map(str, all_fastq_paths))
            ]
        )
