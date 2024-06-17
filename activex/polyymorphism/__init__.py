
from abc import ABC

class PatternX(ABC):
    def __init__(self):
        pass

class ManagerWorker(PatternX):
    pass


class DivideAndConquer(PatternX):
    pass


class SkeletonX(ABC):
    pass

class XoloSkeleton(SkeletonX):
    pass

class ComposeFileSkeleton(SkeletonX):
    pass

class KubernetesSkeleton(SkeletonX):
    pass


