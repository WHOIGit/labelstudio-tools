import pytest

from labelstudio_tools.taskclass import (
    BBox, ResultField, BaseRegion, BaseAnnotation, BaseTask,
)


# ---------------------------------------------------------------------------
# Concrete test implementations (ISIIS-like patterns)
# ---------------------------------------------------------------------------

class SampleRoi(BaseRegion):
    """Test region: bounding box + taxonomy class + boolean flags."""
    ROI_FIELD = ResultField(from_name='roi', to_name='image', result_type='rectangle')
    CLASS_FIELD = ResultField(from_name='roi_class', to_name='image', result_type='taxonomy')
    BOOLEANS_FIELD = ResultField(from_name='roi_booleans', to_name='image', result_type='choices')

    def __init__(self, bbox, class_taxonpath,
                 verified=False, checkme=False,
                 score=None, original_width=None, original_height=None):
        super().__init__(score, original_width, original_height)
        self.bbox = bbox
        self.class_taxonpath = class_taxonpath
        self.verified = verified
        self.checkme = checkme

    def as_result_dicts(self, region_id=None):
        results = []
        roi = self.ROI_FIELD.build(
            self.bbox.as_result_value(), region_id, self.score,
            self.original_width, self.original_height)
        results.append(roi)

        if self.class_taxonpath:
            cls_result = self.CLASS_FIELD.build(
                {'taxonomy': [self.class_taxonpath]}, region_id)
            results.append(cls_result)

        choices = []
        if self.verified:
            choices.append('roi-verified')
        if self.checkme:
            choices.append('roi-checkme')
        if choices:
            bool_result = self.BOOLEANS_FIELD.build({'choices': choices}, region_id)
            results.append(bool_result)

        return results

    def to_dict(self):
        return {
            'bbox': self.bbox.to_dict(),
            'class_taxonpath': self.class_taxonpath,
            'verified': self.verified,
            'checkme': self.checkme,
            'score': self.score,
            'original_width': self.original_width,
            'original_height': self.original_height,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            BBox.from_dict(d['bbox']), d['class_taxonpath'],
            d.get('verified', False), d.get('checkme', False),
            d.get('score'), d.get('original_width'), d.get('original_height'))


class SampleAnnotation(BaseAnnotation):
    """Test annotation with frame-level training/holdout flags."""
    DATASET_FIELD = ResultField('frame_dataset', 'image', 'choices')

    def __init__(self, training=False, holdout=False, regions=None,
                 score=None, model_version=None,
                 original_width=None, original_height=None):
        super().__init__(regions, score, model_version, original_width, original_height)
        self.training = training
        self.holdout = holdout

    def as_dict(self, task_id=None, force=None):
        output = super().as_dict(task_id, force)
        frame_results = []
        choices = []
        if self.training:
            choices.append('training')
        if self.holdout:
            choices.append('holdout')
        if choices:
            frame_results.append(self.DATASET_FIELD.build({'choices': choices}))
        output['result'] = frame_results + output['result']
        return output

    def to_dict(self):
        return {
            'training': self.training,
            'holdout': self.holdout,
            'regions': [r.to_dict() for r in self.regions],
            'score': self.score,
            'model_version': self.model_version,
            'original_width': self.original_width,
            'original_height': self.original_height,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            d.get('training', False), d.get('holdout', False),
            [SampleRoi.from_dict(r) for r in d.get('regions', [])],
            d.get('score'), d.get('model_version'),
            d.get('original_width'), d.get('original_height'))


class SampleTask(BaseTask):
    """Test task with cruise/media/frame/image fields."""

    def __init__(self, cruise, media, frame, image, annotations=None):
        super().__init__(annotations)
        self.cruise = cruise
        self.media = media
        self.frame = frame
        self.image = image

    def data_dict(self):
        return {
            'cruise': self.cruise,
            'media': self.media,
            'frame': self.frame,
            'image': self.image,
        }

    def to_dict(self):
        return {
            **self.data_dict(),
            'annotations': [a.to_dict() for a in self.annotations],
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            d['cruise'], d['media'], d['frame'], d['image'],
            [SampleAnnotation.from_dict(a) for a in d.get('annotations', [])])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_roi(score=None, verified=False, checkme=False):
    bbox = BBox(10.0, 20.0, 30.0, 40.0)
    return SampleRoi(
        bbox, class_taxonpath=['Animalia', 'Arthropoda', 'Copepoda'],
        verified=verified, checkme=checkme, score=score)


def _make_annotation(with_rois=True, score=None, model_version=None,
                     training=False, holdout=False):
    regions = [_make_roi(score=0.9), _make_roi(score=0.8)] if with_rois else []
    return SampleAnnotation(
        training=training, holdout=holdout, regions=regions,
        score=score, model_version=model_version)


# ---------------------------------------------------------------------------
# Tests: BBox
# ---------------------------------------------------------------------------

class TestBBox:
    def test_init_and_as_result_value(self):
        bbox = BBox(10, 20, 30, 40)
        val = bbox.as_result_value()
        assert val == {'x': 10, 'y': 20, 'width': 30, 'height': 40}
        assert 'rotation' not in val

    def test_as_result_value_with_rotation(self):
        bbox = BBox(10, 20, 30, 40, rotation=45.0)
        val = bbox.as_result_value()
        assert val['rotation'] == 45.0

    def test_from_yolo(self):
        bbox = BBox.from_yolo(0.5, 0.5, 0.5, 0.5)
        assert bbox.x == 25.0
        assert bbox.y == 25.0
        assert bbox.width == 50.0
        assert bbox.height == 50.0

    def test_from_yolo_corner(self):
        bbox = BBox.from_yolo(0.25, 0.25, 0.5, 0.5)
        assert bbox.x == 0.0
        assert bbox.y == 0.0

    def test_from_pixels(self):
        bbox = BBox.from_pixels(100, 200, 50, 50, 1000, 1000)
        assert bbox.x == 10.0
        assert bbox.y == 20.0
        assert bbox.width == 5.0
        assert bbox.height == 5.0

    def test_to_pixels_roundtrip(self):
        bbox = BBox(10.0, 20.0, 30.0, 40.0)
        px = bbox.to_pixels(1000, 500)
        assert px == (100.0, 100.0, 300.0, 200.0)
        bbox2 = BBox.from_pixels(*px, 1000, 500)
        assert abs(bbox2.x - bbox.x) < 1e-9
        assert abs(bbox2.y - bbox.y) < 1e-9

    def test_to_dict_from_dict(self):
        bbox = BBox(1.5, 2.5, 3.5, 4.5, rotation=15.0)
        d = bbox.to_dict()
        bbox2 = BBox.from_dict(d)
        assert bbox2.x == bbox.x
        assert bbox2.y == bbox.y
        assert bbox2.width == bbox.width
        assert bbox2.height == bbox.height
        assert bbox2.rotation == bbox.rotation

    def test_to_dict_no_rotation(self):
        bbox = BBox(1, 2, 3, 4)
        d = bbox.to_dict()
        assert 'rotation' not in d


# ---------------------------------------------------------------------------
# Tests: ResultField
# ---------------------------------------------------------------------------

class TestResultField:
    def test_build_basic(self):
        rf = ResultField('roi', 'image', 'rectangle')
        result = rf.build({'x': 10, 'y': 20, 'width': 30, 'height': 40})
        assert result['type'] == 'rectangle'
        assert result['from_name'] == 'roi'
        assert result['to_name'] == 'image'
        assert result['value'] == {'x': 10, 'y': 20, 'width': 30, 'height': 40}
        assert 'id' not in result
        assert 'score' not in result

    def test_build_with_region_id(self):
        rf = ResultField('roi', 'image', 'rectangle')
        result = rf.build({'x': 10}, region_id='region_001')
        assert result['id'] == 'region_001'

    def test_build_with_score_and_dimensions(self):
        rf = ResultField('roi', 'image', 'rectangle')
        result = rf.build({'x': 10}, score=0.95,
                          original_width=1920, original_height=1080)
        assert result['score'] == 0.95
        assert result['original_width'] == 1920
        assert result['original_height'] == 1080


# ---------------------------------------------------------------------------
# Tests: BaseRegion (via SampleRoi)
# ---------------------------------------------------------------------------

class TestBaseRegion:
    def test_sample_roi_as_result_dicts_full(self):
        roi = _make_roi(score=0.9, verified=True, checkme=False)
        results = roi.as_result_dicts(region_id='region_000')
        # bbox + taxonomy + booleans (verified)
        assert len(results) == 3
        assert results[0]['type'] == 'rectangle'
        assert results[0]['id'] == 'region_000'
        assert results[0]['score'] == 0.9
        assert results[1]['type'] == 'taxonomy'
        assert results[1]['value']['taxonomy'] == [['Animalia', 'Arthropoda', 'Copepoda']]
        assert results[2]['type'] == 'choices'
        assert 'roi-verified' in results[2]['value']['choices']

    def test_sample_roi_minimal(self):
        bbox = BBox(10, 20, 30, 40)
        roi = SampleRoi(bbox, ['Animalia', 'Copepoda'])
        results = roi.as_result_dicts()
        # bbox + taxonomy only, no booleans
        assert len(results) == 2
        assert results[0]['type'] == 'rectangle'
        assert results[1]['type'] == 'taxonomy'

    def test_sample_roi_no_taxonomy(self):
        bbox = BBox(10, 20, 30, 40)
        roi = SampleRoi(bbox, [])
        results = roi.as_result_dicts()
        assert len(results) == 1
        assert results[0]['type'] == 'rectangle'

    def test_to_dict_from_dict_roundtrip(self):
        roi = _make_roi(score=0.85, verified=True, checkme=True)
        d = roi.to_dict()
        roi2 = SampleRoi.from_dict(d)
        assert roi2.bbox.x == roi.bbox.x
        assert roi2.class_taxonpath == roi.class_taxonpath
        assert roi2.verified == roi.verified
        assert roi2.checkme == roi.checkme
        assert roi2.score == roi.score


# ---------------------------------------------------------------------------
# Tests: BaseAnnotation (via SampleAnnotation)
# ---------------------------------------------------------------------------

class TestBaseAnnotation:
    def test_is_prediction_with_model_version(self):
        ann = _make_annotation(model_version='yolov8-v1')
        assert ann.is_prediction() is True

    def test_is_prediction_with_score_only(self):
        ann = _make_annotation(score=0.75)
        assert ann.is_prediction() is True

    def test_is_not_prediction(self):
        ann = SampleAnnotation(regions=[_make_roi()])
        assert ann.is_prediction() is False

    def test_score_from_regions_mean(self):
        ann = _make_annotation()  # scores 0.9, 0.8
        assert ann.score_from_regions('mean') == pytest.approx(0.85)

    def test_score_from_regions_min(self):
        ann = _make_annotation()
        assert ann.score_from_regions('min') == pytest.approx(0.8)

    def test_score_from_regions_max(self):
        ann = _make_annotation()
        assert ann.score_from_regions('max') == pytest.approx(0.9)

    def test_score_from_regions_none(self):
        ann = SampleAnnotation(regions=[_make_roi()])  # no score on region
        assert ann.score_from_regions() is None

    def test_score_from_regions_bad_method(self):
        ann = _make_annotation()
        with pytest.raises(ValueError, match="Unknown method"):
            ann.score_from_regions('median')

    def test_as_dict_annotation(self):
        ann = SampleAnnotation(regions=[_make_roi()])
        output = ann.as_dict()
        assert 'score' not in output
        assert 'model_version' not in output
        assert 'result' in output

    def test_as_dict_prediction(self):
        ann = _make_annotation(score=0.9, model_version='model-v1')
        output = ann.as_dict()
        assert output['score'] == 0.9
        assert output['model_version'] == 'model-v1'

    def test_as_dict_with_task_id(self):
        ann = SampleAnnotation(regions=[_make_roi()])
        output = ann.as_dict(task_id=42)
        assert output['task'] == 42

    def test_as_dict_force_prediction(self):
        ann = SampleAnnotation(regions=[_make_roi()])
        output = ann.as_dict(force='prediction')
        assert output['score'] == 0.50
        assert output['model_version'] == 'dummy-model-version'

    def test_as_dict_force_prediction_with_values(self):
        ann = _make_annotation(score=0.7, model_version='real-model')
        output = ann.as_dict(force='prediction')
        assert output['score'] == 0.7
        assert output['model_version'] == 'real-model'

    def test_as_dict_force_annotation(self):
        ann = _make_annotation(score=0.9, model_version='model-v1')
        output = ann.as_dict(force='annotation')
        assert 'score' not in output
        assert 'model_version' not in output

    def test_frame_level_results(self):
        ann = SampleAnnotation(training=True, holdout=True, regions=[_make_roi()])
        output = ann.as_dict()
        # Frame-level choices should be first in result list
        assert output['result'][0]['type'] == 'choices'
        assert output['result'][0]['from_name'] == 'frame_dataset'
        assert 'training' in output['result'][0]['value']['choices']
        assert 'holdout' in output['result'][0]['value']['choices']

    def test_original_dimensions_propagate_to_regions(self):
        roi = _make_roi(score=0.9)
        ann = SampleAnnotation(regions=[roi], original_width=1920, original_height=1080)
        output = ann.as_dict()
        rect_result = [r for r in output['result'] if r['type'] == 'rectangle'][0]
        assert rect_result['original_width'] == 1920
        assert rect_result['original_height'] == 1080

    def test_to_dict_from_dict_roundtrip(self):
        ann = _make_annotation(score=0.85, model_version='v2', training=True)
        d = ann.to_dict()
        ann2 = SampleAnnotation.from_dict(d)
        assert ann2.training == ann.training
        assert ann2.score == ann.score
        assert ann2.model_version == ann.model_version
        assert len(ann2.regions) == len(ann.regions)


# ---------------------------------------------------------------------------
# Tests: BaseTask (via SampleTask)
# ---------------------------------------------------------------------------

class TestBaseTask:
    def test_data_dict(self):
        task = SampleTask('cruise01', 'media01', 100, 'http://img.png')
        assert task.data_dict() == {
            'cruise': 'cruise01', 'media': 'media01',
            'frame': 100, 'image': 'http://img.png',
        }

    def test_as_new_taskdata_dict_basic(self):
        task = SampleTask('c', 'm', 1, 'img.png')
        td = task.as_new_taskdata_dict()
        assert 'data' in td
        assert td['data']['cruise'] == 'c'
        assert 'predictions' not in td
        assert 'annotations' not in td

    def test_as_new_taskdata_dict_with_predictions(self):
        pred_ann = _make_annotation(score=0.9, model_version='model-v1')
        task = SampleTask('c', 'm', 1, 'img.png', annotations=[pred_ann])
        td = task.as_new_taskdata_dict(predictions_key='predictions')
        assert 'predictions' in td
        assert len(td['predictions']) == 1
        assert td['predictions'][0]['score'] == 0.9

    def test_as_new_taskdata_dict_with_annotations(self):
        manual_ann = SampleAnnotation(regions=[_make_roi()])
        task = SampleTask('c', 'm', 1, 'img.png', annotations=[manual_ann])
        td = task.as_new_taskdata_dict(annotations_key='annotations')
        assert 'annotations' in td
        assert len(td['annotations']) == 1

    def test_as_new_taskdata_dict_mixed(self):
        pred = _make_annotation(score=0.9, model_version='model-v1')
        manual = SampleAnnotation(regions=[_make_roi()])
        task = SampleTask('c', 'm', 1, 'img.png', annotations=[pred, manual])
        td = task.as_new_taskdata_dict(
            predictions_key='predictions', annotations_key='annotations')
        assert len(td['predictions']) == 1
        assert len(td['annotations']) == 1

    def test_as_new_taskdata_dict_no_matching(self):
        # Only predictions, but asking for annotations key
        pred = _make_annotation(score=0.9, model_version='model-v1')
        task = SampleTask('c', 'm', 1, 'img.png', annotations=[pred])
        td = task.as_new_taskdata_dict(annotations_key='annotations')
        assert 'annotations' not in td

    def test_to_dict_from_dict_roundtrip(self):
        roi = _make_roi(score=0.95)
        ann = SampleAnnotation(training=True, regions=[roi], model_version='v1', score=0.95)
        task = SampleTask('cruise01', 'media01', 42, 'http://img.png', annotations=[ann])
        d = task.to_dict()
        task2 = SampleTask.from_dict(d)
        assert task2.cruise == task.cruise
        assert task2.frame == task.frame
        assert len(task2.annotations) == 1
        assert len(task2.annotations[0].regions) == 1
        assert task2.annotations[0].regions[0].score == 0.95
