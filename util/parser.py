from pyparsing import Word, alphanums, quotedString, oneOf, infixNotation, opAssoc, ParseException
import logging

logger = logging.getLogger('logger')
# Define grammar elements
# Allow common identifier characters without requiring quotes (e.g., hyphens, underscores, slashes, colons, plus, dots)
identifier = Word(alphanums + "-_./:+*")
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
    # Quick parentheses balance check ignoring text inside quotes
    def _balanced_parentheses(s: str) -> bool:
        depth = 0
        in_quote = False
        q = ''
        for ch in s:
            if in_quote:
                if ch == q:
                    in_quote = False
                continue
            if ch in ('"', '\''):
                in_quote = True
                q = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth < 0:
                    return False
        return depth == 0 and not in_quote

    if not _balanced_parentheses(expression_str):
        logger.info('Error parsing expression: unbalanced parentheses or unterminated quotes')
        return [], False

    try:
        parsed = expression.parseString(expression_str, parseAll=True)
        return parsed[0], True
    except ParseException as e:
        # Build a caret marker to indicate where parsing failed
        try:
            line, col = e.line, e.column
            caret = ' ' * (col - 1) + '^'
            logger.info('Error parsing expression at column ' + str(col) + ':')
            logger.info(line)
            logger.info(caret)
            logger.info(str(e))
        except Exception:
            logger.info('Error parsing expression: ' + str(e))
        return [], False
