package memfs

import "fmt"

type treeNode struct {
	data        Comparable
	left, right *treeNode
	h           int
}

func newNode(key Comparable) *treeNode {
	return &treeNode{
		data: key,
		h:    1,
	}
}

func height(n *treeNode) int {
	if n == nil {
		return 0
	}
	return n.h
}

func rotateRight(n *treeNode) *treeNode {
	node := n.left
	n.left = node.right

	node.right = n

	n.h = max(height(n.left), height(n.right)) + 1
	node.h = max(height(node.left), height(node.right)) + 1

	return node
}

func rotateLeft(n *treeNode) *treeNode {
	node := n.right
	n.right = node.left

	node.left = n

	n.h = max(height(n.left), height(n.right)) + 1
	node.h = max(height(node.left), height(node.right)) + 1

	return node
}

func leftRightRotate(n *treeNode) *treeNode {
	n.left = rotateLeft(n.left)
	n = rotateRight(n)
	return n
}

func rightLeftRotate(n *treeNode) *treeNode {
	n.right = rotateRight(n.right)
	n = rotateLeft(n)
	return n
}

func (n *treeNode) balanceFactor() int {
	if n == nil {
		return 0
	}
	return height(n.left) - height(n.right)
}

func insert(n *treeNode, key Comparable) (*treeNode, error) {
	var err error

	if n == nil {
		return newNode(key), nil
	}

	if key.Compare(n.data) < 0 {
		n.left, err = insert(n.left, key)
	} else if key.Compare(n.data) == 0 {
		n.data = key
		return n, nil
	} else {
		n.right, err = insert(n.right, key)
	}

	n.h = max(height(n.left), height(n.right)) + 1

	bal := n.balanceFactor()
	switch {
	case bal > 1:
		if key.Compare(n.left.data) < 0 {
			return rotateRight(n), err
		}
		n = leftRightRotate(n)
		return n, err
	case bal < -1:
		if key.Compare(n.right.data) < 0 {
			n = rightLeftRotate(n)
			return n, err
		}
		return rotateLeft(n), err
	}

	return n, err
}

func remove(n *treeNode, key Comparable) (*treeNode, error) {
	var err error

	if n == nil {
		return nil, fmt.Errorf("Key not found in the tree: %v", key)
	}

	if key.Compare(n.data) < 0 {
		n.left, err = remove(n.left, key)
	} else if key.Compare(n.data) == 0 {
		if n.left == nil || n.right == nil {
			var tmp *treeNode
			if n.left == nil {
				tmp = n.right
			} else {
				tmp = n.left
			}

			if tmp == nil {
				tmp = n
				n = nil
			} else {
				n = tmp
			}
		} else {
			tmp := min(n.right)
			n.data = tmp.data
			n.right, err = remove(n.right, tmp.data)
		}
	} else {
		n.right, err = remove(n.right, key)
	}
	if n == nil {
		return n, err
	}

	n.h = 1 + max(height(n.left), height(n.right))

	bal := n.balanceFactor()
	switch {
	case bal > 1:
		if n.left.balanceFactor() >= 0 {
			return rotateRight(n), err
		}
		n = leftRightRotate(n)
		return n, err
	case bal < -1:
		if n.right.balanceFactor() <= 0 {
			return rotateLeft(n), err
		}
		n.right = rightLeftRotate(n)
		return n, err
	}

	return n, err
}

func get(n *treeNode, key Comparable) Comparable {
	var result int
	for n != nil {
		switch result = key.Compare(n.data); {
		case result == 0:
			return n.data
		case result > 0:
			n = n.right
		case result < 0:
			n = n.left
		}
	}
	return nil
}

func min(n *treeNode) *treeNode {
	curr := n
	for curr.left != nil {
		curr = curr.left
	}
	return curr
}

func maxN(n *treeNode) *treeNode {
	curr := n
	for curr.right != nil {
		curr = curr.right
	}
	return curr
}

func inOrder(n *treeNode) []Comparable {
	if n == nil {
		return nil
	}
	ret := []Comparable{}
	ret = append(ret, inOrder(n.left)...)
	ret = append(ret, n.data)
	ret = append(ret, inOrder(n.right)...)
	return ret
}
