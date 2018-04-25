#
# LSST Data Management System
# Copyright 2018 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""Simple unit test for expr_parser/parserLex module.
"""

import re
import unittest

from lsst.pipe.supertask.expr_parser import parserYacc
import lsst.utils.tests


class ParserLexTestCase(unittest.TestCase):
    """A test case for ParserYacc
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testInstantiate(self):
        """Tests for making ParserLex instances
        """
        parser = parserYacc.ParserYacc()

    def testParseLiteral(self):
        """Tests for literals (strings/numbers)
        """
        parser = parserYacc.ParserYacc()

        tree = parser.parse('1')
        self.assertIsInstance(tree, parserYacc.NumericLiteral)
        self.assertEqual(tree.value, '1')

        tree = parser.parse('.5e-2')
        self.assertIsInstance(tree, parserYacc.NumericLiteral)
        self.assertEqual(tree.value, '.5e-2')

        tree = parser.parse("'string'")
        self.assertIsInstance(tree, parserYacc.StringLiteral)
        self.assertEqual(tree.value, 'string')

    def testParseIdentifiers(self):
        """Tests for identifiers
        """
        parser = parserYacc.ParserYacc()

        tree = parser.parse('a')
        self.assertIsInstance(tree, parserYacc.Identifier)
        self.assertEqual(tree.value, 'a')

        tree = parser.parse('a.b')
        self.assertIsInstance(tree, parserYacc.Identifier)
        self.assertEqual(tree.value, 'a.b')

    def testParseParens(self):
        """Tests for identifiers
        """
        parser = parserYacc.ParserYacc()

        tree = parser.parse('(a)')
        self.assertIsInstance(tree, parserYacc.Parens)
        self.assertIsInstance(tree.value, parserYacc.Identifier)
        self.assertEqual(tree.value.value, 'a')

    def testUnaryOps(self):
        """Tests for unary plus and minus
        """
        parser = parserYacc.ParserYacc()

        tree = parser.parse('+a')
        self.assertIsInstance(tree, parserYacc.UnaryOp)
        self.assertEqual(tree.op, '+')
        self.assertIsInstance(tree.operand, parserYacc.Identifier)
        self.assertEqual(tree.operand.value, 'a')

        tree = parser.parse('- x.y')
        self.assertIsInstance(tree, parserYacc.UnaryOp)
        self.assertEqual(tree.op, '-')
        self.assertIsInstance(tree.operand, parserYacc.Identifier)
        self.assertEqual(tree.operand.value, 'x.y')

    def testBinaryOps(self):
        """Tests for binary operators
        """
        parser = parserYacc.ParserYacc()

        tree = parser.parse('a + b')
        self.assertIsInstance(tree, parserYacc.BinaryOp)
        self.assertEqual(tree.op, '+')
        self.assertIsInstance(tree.left, parserYacc.Identifier)
        self.assertIsInstance(tree.right, parserYacc.Identifier)
        self.assertEqual(tree.left.value, 'a')
        self.assertEqual(tree.right.value, 'b')

        tree = parser.parse('a - 2')
        self.assertIsInstance(tree, parserYacc.BinaryOp)
        self.assertEqual(tree.op, '-')
        self.assertIsInstance(tree.left, parserYacc.Identifier)
        self.assertIsInstance(tree.right, parserYacc.NumericLiteral)
        self.assertEqual(tree.left.value, 'a')
        self.assertEqual(tree.right.value, '2')

        tree = parser.parse('2 * 2')
        self.assertIsInstance(tree, parserYacc.BinaryOp)
        self.assertEqual(tree.op, '*')
        self.assertIsInstance(tree.left, parserYacc.NumericLiteral)
        self.assertIsInstance(tree.right, parserYacc.NumericLiteral)
        self.assertEqual(tree.left.value, '2')
        self.assertEqual(tree.right.value, '2')

        tree = parser.parse('1.e5/2')
        self.assertIsInstance(tree, parserYacc.BinaryOp)
        self.assertEqual(tree.op, '/')
        self.assertIsInstance(tree.left, parserYacc.NumericLiteral)
        self.assertIsInstance(tree.right, parserYacc.NumericLiteral)
        self.assertEqual(tree.left.value, '1.e5')
        self.assertEqual(tree.right.value, '2')

        tree = parser.parse('333%76')
        self.assertIsInstance(tree, parserYacc.BinaryOp)
        self.assertEqual(tree.op, '%')
        self.assertIsInstance(tree.left, parserYacc.NumericLiteral)
        self.assertIsInstance(tree.right, parserYacc.NumericLiteral)
        self.assertEqual(tree.left.value, '333')
        self.assertEqual(tree.right.value, '76')

    def testIsIn(self):
        """Tests for IN
        """
        parser = parserYacc.ParserYacc()

        tree = parser.parse("a in (1,2,'X')")
        self.assertIsInstance(tree, parserYacc.IsIn)
        self.assertFalse(tree.not_in)
        self.assertIsInstance(tree.lhs, parserYacc.Identifier)
        self.assertEqual(tree.lhs.value, 'a')
        self.assertIsInstance(tree.values, list)
        self.assertEqual(len(tree.values), 3)
        self.assertIsInstance(tree.values[0], parserYacc.NumericLiteral)
        self.assertEqual(tree.values[0].value, '1')
        self.assertIsInstance(tree.values[1], parserYacc.NumericLiteral)
        self.assertEqual(tree.values[1].value, '2')
        self.assertIsInstance(tree.values[2], parserYacc.StringLiteral)
        self.assertEqual(tree.values[2].value, 'X')

        tree = parser.parse("10 not in (1000)")
        self.assertIsInstance(tree, parserYacc.IsIn)
        self.assertTrue(tree.not_in)
        self.assertIsInstance(tree.lhs, parserYacc.NumericLiteral)
        self.assertEqual(tree.lhs.value, '10')
        self.assertIsInstance(tree.values, list)
        self.assertEqual(len(tree.values), 1)
        self.assertIsInstance(tree.values[0], parserYacc.NumericLiteral)
        self.assertEqual(tree.values[0].value, '1000')

    def testCompareOps(self):
        """Tests for comparison operators
        """
        parser = parserYacc.ParserYacc()

        for op in ('=', '!=', '<', '<=', '>', '>='):
            tree = parser.parse('a {} 10'.format(op))
            self.assertIsInstance(tree, parserYacc.BinaryOp)
            self.assertEqual(tree.op, op)
            self.assertIsInstance(tree.left, parserYacc.Identifier)
            self.assertIsInstance(tree.right, parserYacc.NumericLiteral)
            self.assertEqual(tree.left.value, 'a')
            self.assertEqual(tree.right.value, '10')

    def testBoolOps(self):
        """Tests for boolean operators
        """
        parser = parserYacc.ParserYacc()

        for op in ('OR', 'XOR', 'AND'):
            tree = parser.parse('a {} b'.format(op))
            self.assertIsInstance(tree, parserYacc.BinaryOp)
            self.assertEqual(tree.op, op)
            self.assertIsInstance(tree.left, parserYacc.Identifier)
            self.assertIsInstance(tree.right, parserYacc.Identifier)
            self.assertEqual(tree.left.value, 'a')
            self.assertEqual(tree.right.value, 'b')

        tree = parser.parse('NOT b')
        self.assertIsInstance(tree, parserYacc.UnaryOp)
        self.assertEqual(tree.op, 'NOT')
        self.assertIsInstance(tree.operand, parserYacc.Identifier)
        self.assertEqual(tree.operand.value, 'b')

    def testExpression(self):
        """Test for more or less complete expression"""
        parser = parserYacc.ParserYacc()

        expr = ("((camera='HSC' AND sensor != 9) OR camera='CFHT') "
                "AND tract=8766 AND patch.cell_x > 5 AND "
                "patch.cell_y < 4 AND abstract_filter='i'")

        tree = parser.parse(expr)
        self.assertIsInstance(tree, parserYacc.BinaryOp)
        self.assertEqual(tree.op, 'AND')
        self.assertIsInstance(tree.left, parserYacc.BinaryOp)
        # AND is left-associative, so right operand will be the
        # last sub-expressions
        self.assertIsInstance(tree.right, parserYacc.BinaryOp)
        self.assertEqual(tree.right.op, '=')
        self.assertIsInstance(tree.right.left, parserYacc.Identifier)
        self.assertEqual(tree.right.left.value, 'abstract_filter')
        self.assertIsInstance(tree.right.right, parserYacc.StringLiteral)
        self.assertEqual(tree.right.right.value, 'i')

    def testException(self):
        """Test for exceptional cases"""

        def _assertExc(exc, expr, token, pos, lineno, posInLine):
            """Check exception attribute values"""
            self.assertEqual(exc.expression, expr)
            self.assertEqual(exc.token, token)
            self.assertEqual(exc.pos, pos)
            self.assertEqual(exc.lineno, lineno)
            self.assertEqual(exc.posInLine, posInLine)

        parser = parserYacc.ParserYacc()

        with self.assertRaises(parserYacc.EOFError):
            parser.parse("")

        expr = "(1, 2, 3)"
        with self.assertRaises(parserYacc.ParseError) as catcher:
            parser.parse(expr)
        _assertExc(catcher.exception, expr, ",", 2, 1, 2)

        expr = "\n(1\n,\n 2, 3)"
        with self.assertRaises(parserYacc.ParseError) as catcher:
            parser.parse(expr)
        _assertExc(catcher.exception, expr, ",", 4, 3, 0)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
