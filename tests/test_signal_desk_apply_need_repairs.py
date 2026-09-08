import sqlite3
import pytest
from scripts.pif_signal_desk_apply_need_repairs import apply_verified
from research_factory.signal_desk_rebuild_dispatch import initialize_dispatch_schema,enqueue_task,acquire_lease,fail_attempt_semantically,list_attempts


def setup(directory,detail='source limitation requires recovery need'):
    db=sqlite3.connect(directory/'dispatch.sqlite');db.row_factory=sqlite3.Row;initialize_dispatch_schema(db)
    enqueue_task(db,task_key='prefix:packet',task_type='gold',payload={})
    lease=acquire_lease(db,lease_owner='test',lease_seconds=1800)
    fail_attempt_semantically(db,attempt_id=lease['current_attempt_id'],lease_owner=lease['lease_owner'],lease_generation=lease['lease_generation'],failure_code='contract',failure_detail=detail)
    db.close()


def test_application_preserves_failed_attempt_and_is_idempotent(tmp_path):
    setup(tmp_path);p={'packet_sha256':'packet'};fixed={'events':[]};proof={'gold_accepted':False}
    assert not apply_verified(tmp_path,p,fixed,proof,'prefix')['applied']
    assert not (tmp_path/'packet.result.json').exists()
    assert apply_verified(tmp_path,p,fixed,proof,'prefix',execute=True)['applied']
    assert apply_verified(tmp_path,p,fixed,proof,'prefix',execute=True)['already_applied']
    db=sqlite3.connect(tmp_path/'dispatch.sqlite')
    rows=db.execute('SELECT status,semantic_failure_detail FROM signal_desk_rebuild_attempts ORDER BY id').fetchall();db.close()
    assert [r[0] for r in rows]==['terminal_failed','succeeded']
    assert rows[0][1]=='source limitation requires recovery need'


def test_unrelated_failure_cannot_be_resurrected(tmp_path):
    setup(tmp_path,'different error')
    with pytest.raises(ValueError,match='inspected'):apply_verified(tmp_path,{'packet_sha256':'packet'},{},{},'prefix',execute=True)
    assert not (tmp_path/'packet.result.json').exists()
