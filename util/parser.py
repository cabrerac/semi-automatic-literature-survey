from pyparsing import Word, alphanums, quotedString, oneOf, infixNotation, opAssoc, ParseException
import logging

logger = logging.getLogger('logger')
# Define grammar elements
identifier = Word(alphanums)
string_literal = quotedString.setParseAction(lambda t: t[0][1:-1])
and_operator = oneOf("<AND>")
or_operator = oneOf("<OR>")

# Define expression grammar
expression = infixNotation(
    identifier | string_literal,
    [
        (and_operator, 2, opAssoc.LEFT),
        (or_operator, 2, opAssoc.LEFT),
    ]
)


# Parse boolean expression function
def parse_boolean_expression(expression_str):
    try:
        parsed = expression.parseString(expression_str, parseAll=True)
        return parsed[0], True
    except ParseException as e:
        logger.info('Error parsing expression: ' + str(e))
        return [], False
