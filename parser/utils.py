from io import StringIO
from antlr4 import RuleContext

def get_children_with_type(node: RuleContext, ttype: type) -> list[RuleContext]: # fixme: not all have children
    children = []
    for child in node.getChildren():
        if isinstance(child, ttype):
            children.append(child)
    return children


def find_children_with_type(node: RuleContext, ttype: type, skip_node_types: list[type] = []) -> list[RuleContext]:
    """
    Find all children of a node with a specific type using DFS.
    """
    def _find_children_with_type(node: RuleContext, ttype: type, children):
        if isinstance(node, ttype):
            children.append(node)
        
        # 如果当前节点是需要跳过的节点类型，则不再继续向下搜索
        for skip_node_type in skip_node_types:
            if isinstance(node, skip_node_type):
                return children
            
        # 如果当前节点是非叶子节点，则继续向下搜索
        if isinstance(node, RuleContext):
            for child in node.getChildren():
                _find_children_with_type(child, ttype, children)

        return children
        
    
    return _find_children_with_type(node, ttype, [])

def get_altered_text(node: RuleContext, alter_type: type, alter_list: list[tuple[str, str]]) -> str:
    """
    Find all children of a node with a specific type using DFS.
    """
    def _get_altered_text(node: RuleContext, alter_type: type, buf: StringIO):
        if isinstance(node, alter_type):
            for alter in alter_list:
                if node.getText().strip() == alter[0]:
                    buf.write(alter[1])
                    return
        if isinstance(node, RuleContext):
            for child in node.getChildren():
                _get_altered_text(child, alter_type, buf)
        else:
            buf.write(node.getText())
            
    with StringIO() as buf:
        _get_altered_text(node, alter_type, buf)
        return buf.getvalue()