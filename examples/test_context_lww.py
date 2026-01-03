#!/usr/bin/env python3
"""
Test Context and WorkflowContext LWW (Last-Write-Wins) functionality.

Verifies that:
1. Local usage (without timestamps) still works correctly
2. LWW conflict resolution works with timestamps
3. Source node tiebreaker works for equal timestamps
4. Existing callers without new parameters still work
"""

import asyncio
from hyperscale.core.state.context import Context
from hyperscale.core.state.workflow_context import WorkflowContext


# =============================================================================
# Test utilities
# =============================================================================

try:
    _loop = asyncio.get_running_loop()
except RuntimeError:
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)


def run_async(coro):
    """Run an async coroutine synchronously."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


class TestResult:
    """Track test results."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def record_pass(self, name: str):
        self.passed += 1
        print(f"  ✓ {name}")
    
    def record_fail(self, name: str, error: Exception):
        self.failed += 1
        self.errors.append((name, error))
        print(f"  ✗ {name}: {error}")
    
    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"Results: {self.passed}/{total} passed")
        if self.errors:
            print(f"\nFailed tests:")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
        return self.failed == 0


# =============================================================================
# WorkflowContext Local Usage Tests
# =============================================================================

def test_workflow_context_basic_set_get():
    """Basic set/get without timestamps (local usage)."""
    async def _test():
        ctx = WorkflowContext()
        
        await ctx.set("key1", "value1")
        await ctx.set("key2", 42)
        await ctx.set("key3", {"nested": "dict"})
        
        assert ctx.get("key1") == "value1"
        assert ctx.get("key2") == 42
        assert ctx.get("key3") == {"nested": "dict"}
        assert ctx.get("nonexistent") is None
        assert ctx.get("nonexistent", "default") == "default"
    
    run_async(_test())


def test_workflow_context_overwrite_without_timestamp():
    """Overwriting values without timestamps (local usage)."""
    async def _test():
        ctx = WorkflowContext()
        
        await ctx.set("key", "original")
        assert ctx.get("key") == "original"
        
        await ctx.set("key", "updated")
        assert ctx.get("key") == "updated"
        
        await ctx.set("key", "final")
        assert ctx.get("key") == "final"
    
    run_async(_test())


def test_workflow_context_dict_method():
    """Test dict() returns context as dictionary."""
    async def _test():
        ctx = WorkflowContext()
        
        await ctx.set("a", 1)
        await ctx.set("b", 2)
        
        result = ctx.dict()
        assert result == {"a": 1, "b": 2}
    
    run_async(_test())


def test_workflow_context_items_method():
    """Test items() iteration."""
    async def _test():
        ctx = WorkflowContext()
        
        await ctx.set("x", 10)
        await ctx.set("y", 20)
        
        items = dict(ctx.items())
        assert items == {"x": 10, "y": 20}
    
    run_async(_test())


# =============================================================================
# WorkflowContext LWW Tests
# =============================================================================

def test_lww_newer_timestamp_wins():
    """Newer timestamp overwrites older."""
    async def _test():
        ctx = WorkflowContext()
        
        await ctx.set("key", "old", timestamp=100)
        assert ctx.get("key") == "old"
        
        await ctx.set("key", "new", timestamp=200)
        assert ctx.get("key") == "new"
    
    run_async(_test())


def test_lww_older_timestamp_rejected():
    """Older timestamp is rejected (stale update)."""
    async def _test():
        ctx = WorkflowContext()
        
        await ctx.set("key", "newer", timestamp=200)
        assert ctx.get("key") == "newer"
        
        # This should be rejected - older timestamp
        await ctx.set("key", "older", timestamp=100)
        assert ctx.get("key") == "newer"  # Still the newer value
    
    run_async(_test())


def test_lww_equal_timestamp_with_source_tiebreaker():
    """Equal timestamps use source_node as tiebreaker."""
    async def _test():
        ctx = WorkflowContext()
        
        # Set with source "manager-a"
        await ctx.set("key", "from-a", timestamp=100, source_node="manager-a")
        assert ctx.get("key") == "from-a"
        
        # Same timestamp, but "manager-b" > "manager-a" lexicographically
        await ctx.set("key", "from-b", timestamp=100, source_node="manager-b")
        assert ctx.get("key") == "from-b"  # b wins (greater node_id)
        
        # Same timestamp, but "manager-a" < "manager-b", should be rejected
        await ctx.set("key", "from-a-again", timestamp=100, source_node="manager-a")
        assert ctx.get("key") == "from-b"  # Still b
    
    run_async(_test())


def test_lww_no_timestamp_always_updates():
    """No timestamp (local write) always updates."""
    async def _test():
        ctx = WorkflowContext()
        
        # Set with timestamp
        await ctx.set("key", "timestamped", timestamp=9999)
        assert ctx.get("key") == "timestamped"
        
        # Local write (no timestamp) should still overwrite
        await ctx.set("key", "local")
        assert ctx.get("key") == "local"
    
    run_async(_test())


def test_lww_get_timestamps():
    """Test get_timestamps() returns stored timestamps."""
    async def _test():
        ctx = WorkflowContext()
        
        await ctx.set("a", 1, timestamp=100)
        await ctx.set("b", 2, timestamp=200)
        await ctx.set("c", 3)  # No timestamp
        
        timestamps = ctx.get_timestamps()
        assert timestamps == {"a": 100, "b": 200}  # c has no timestamp
    
    run_async(_test())


def test_lww_get_sources():
    """Test get_sources() returns stored source nodes."""
    async def _test():
        ctx = WorkflowContext()
        
        await ctx.set("a", 1, timestamp=100, source_node="node-1")
        await ctx.set("b", 2, timestamp=200, source_node="node-2")
        await ctx.set("c", 3)  # No source
        
        sources = ctx.get_sources()
        assert sources == {"a": "node-1", "b": "node-2"}  # c has no source
    
    run_async(_test())


# =============================================================================
# Context Local Usage Tests
# =============================================================================

def test_context_basic_update():
    """Basic update without timestamps."""
    async def _test():
        ctx = Context()
        
        await ctx.update("workflow1", "key1", "value1")
        await ctx.update("workflow1", "key2", "value2")
        await ctx.update("workflow2", "key1", "other")
        
        assert ctx["workflow1"].get("key1") == "value1"
        assert ctx["workflow1"].get("key2") == "value2"
        assert ctx["workflow2"].get("key1") == "other"
    
    run_async(_test())


def test_context_dict_method():
    """Test dict() returns nested structure."""
    async def _test():
        ctx = Context()
        
        await ctx.update("wf1", "a", 1)
        await ctx.update("wf1", "b", 2)
        await ctx.update("wf2", "x", 10)
        
        result = ctx.dict()
        assert result == {
            "wf1": {"a": 1, "b": 2},
            "wf2": {"x": 10}
        }
    
    run_async(_test())


def test_context_from_dict():
    """Test from_dict() loads values correctly."""
    async def _test():
        ctx = Context()
        
        await ctx.from_dict("workflow", {"key1": "val1", "key2": "val2"})
        
        assert ctx["workflow"].get("key1") == "val1"
        assert ctx["workflow"].get("key2") == "val2"
    
    run_async(_test())


def test_context_copy():
    """Test copy() copies from another context."""
    async def _test():
        src = Context()
        await src.update("wf1", "key", "value")
        
        dst = Context()
        await dst.copy(src)
        
        assert dst["wf1"].get("key") == "value"
    
    run_async(_test())


def test_context_contains():
    """Test __contains__ for workflow membership."""
    async def _test():
        ctx = Context()
        
        assert "workflow1" not in ctx
        
        await ctx.update("workflow1", "key", "value")
        
        assert "workflow1" in ctx
        assert "workflow2" not in ctx
    
    run_async(_test())


def test_context_get_timestamps():
    """Test get_timestamps() returns nested timestamps."""
    async def _test():
        ctx = Context()
        
        await ctx.update("wf1", "a", 1, timestamp=100)
        await ctx.update("wf1", "b", 2, timestamp=200)
        await ctx.update("wf2", "x", 10, timestamp=300)
        
        timestamps = ctx.get_timestamps()
        assert timestamps == {
            "wf1": {"a": 100, "b": 200},
            "wf2": {"x": 300}
        }
    
    run_async(_test())


# =============================================================================
# Context LWW Tests
# =============================================================================

def test_context_lww_with_timestamp_and_source():
    """Test update() with timestamp and source_node."""
    async def _test():
        ctx = Context()
        
        await ctx.update("wf", "key", "v1", timestamp=100, source_node="node-a")
        assert ctx["wf"].get("key") == "v1"
        
        # Newer timestamp wins
        await ctx.update("wf", "key", "v2", timestamp=200, source_node="node-b")
        assert ctx["wf"].get("key") == "v2"
        
        # Older timestamp rejected
        await ctx.update("wf", "key", "v3", timestamp=150, source_node="node-c")
        assert ctx["wf"].get("key") == "v2"  # Still v2
    
    run_async(_test())


# =============================================================================
# Backwards Compatibility Tests
# =============================================================================

def test_workflow_runner_pattern():
    """
    Test the pattern used in workflow_runner.py:
    
    await asyncio.gather(*[
        context.update(workflow_name, hook_name, value)
        for hook_name, value in workflow_context.items()
    ])
    """
    async def _test():
        ctx = Context()
        
        # Simulate workflow context items
        workflow_context = {"hook1": "result1", "hook2": "result2", "hook3": 42}
        workflow_name = "TestWorkflow"
        
        # This is the exact pattern from workflow_runner.py
        await asyncio.gather(*[
            ctx.update(workflow_name, hook_name, value)
            for hook_name, value in workflow_context.items()
        ])
        
        # Verify all values were set
        assert ctx[workflow_name].get("hook1") == "result1"
        assert ctx[workflow_name].get("hook2") == "result2"
        assert ctx[workflow_name].get("hook3") == 42
    
    run_async(_test())


def test_remote_graph_controller_pattern():
    """
    Test the pattern used in remote_graph_controller.py:
    
    self._node_context[run_id] = await context.from_dict(workflow, values)
    """
    async def _test():
        ctx = Context()
        values = {"key1": "val1", "key2": "val2"}
        
        # This is the exact pattern from remote_graph_controller.py
        result = await ctx.from_dict("workflow", values)
        
        # Verify it returns self for chaining
        assert result is ctx
        
        # Verify values were set
        assert ctx["workflow"].get("key1") == "val1"
        assert ctx["workflow"].get("key2") == "val2"
    
    run_async(_test())


# =============================================================================
# Main test runner
# =============================================================================

def run_all_tests():
    """Run all tests and report results."""
    print("=" * 60)
    print("Context LWW Tests")
    print("=" * 60)
    
    result = TestResult()
    
    tests = [
        # WorkflowContext local usage
        ("WorkflowContext: basic set/get", test_workflow_context_basic_set_get),
        ("WorkflowContext: overwrite without timestamp", test_workflow_context_overwrite_without_timestamp),
        ("WorkflowContext: dict method", test_workflow_context_dict_method),
        ("WorkflowContext: items method", test_workflow_context_items_method),
        
        # WorkflowContext LWW
        ("LWW: newer timestamp wins", test_lww_newer_timestamp_wins),
        ("LWW: older timestamp rejected", test_lww_older_timestamp_rejected),
        ("LWW: equal timestamp with source tiebreaker", test_lww_equal_timestamp_with_source_tiebreaker),
        ("LWW: no timestamp always updates", test_lww_no_timestamp_always_updates),
        ("LWW: get_timestamps", test_lww_get_timestamps),
        ("LWW: get_sources", test_lww_get_sources),
        
        # Context local usage
        ("Context: basic update", test_context_basic_update),
        ("Context: dict method", test_context_dict_method),
        ("Context: from_dict", test_context_from_dict),
        ("Context: copy", test_context_copy),
        ("Context: contains", test_context_contains),
        ("Context: get_timestamps", test_context_get_timestamps),
        
        # Context LWW
        ("Context LWW: with timestamp and source", test_context_lww_with_timestamp_and_source),
        
        # Backwards compatibility
        ("Compat: workflow_runner pattern", test_workflow_runner_pattern),
        ("Compat: remote_graph_controller pattern", test_remote_graph_controller_pattern),
    ]
    
    for name, test_func in tests:
        try:
            test_func()
            result.record_pass(name)
        except Exception as e:
            result.record_fail(name, e)
    
    return result.summary()


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
