// Generated from /Users/yxl/gitwork/spark/spark-manual/src/test/resources/Calculator.g4 by ANTLR 4.7
package org.apache.spark.sql.codegen.gen;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class CalculatorLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, INT=3, DOUBLE=4, WS=5, ID=6, PLUS=7, MINUS=8, MULT=9, 
		DIV=10, LPAR=11, RPAR=12, COMMA=13;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "INT", "DOUBLE", "WS", "ID", "PLUS", "MINUS", "MULT", 
		"DIV", "LPAR", "RPAR", "COMMA"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'sqrt'", "'log'", null, null, null, null, "'+'", "'-'", "'*'", 
		"'/'", "'('", "')'", "','"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, "INT", "DOUBLE", "WS", "ID", "PLUS", "MINUS", "MULT", 
		"DIV", "LPAR", "RPAR", "COMMA"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public CalculatorLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "Calculator.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\17R\b\1\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\4"+
		"\6\4(\n\4\r\4\16\4)\3\5\6\5-\n\5\r\5\16\5.\3\5\3\5\6\5\63\n\5\r\5\16\5"+
		"\64\3\6\6\68\n\6\r\6\16\69\3\6\3\6\3\7\3\7\7\7@\n\7\f\7\16\7C\13\7\3\b"+
		"\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\16\3\16\2\2\17\3\3\5"+
		"\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\3\2\6\3\2\62"+
		";\5\2\13\f\17\17\"\"\5\2C\\aac|\6\2\62;C\\aac|\2V\2\3\3\2\2\2\2\5\3\2"+
		"\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21"+
		"\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2"+
		"\2\2\3\35\3\2\2\2\5\"\3\2\2\2\7\'\3\2\2\2\t,\3\2\2\2\13\67\3\2\2\2\r="+
		"\3\2\2\2\17D\3\2\2\2\21F\3\2\2\2\23H\3\2\2\2\25J\3\2\2\2\27L\3\2\2\2\31"+
		"N\3\2\2\2\33P\3\2\2\2\35\36\7u\2\2\36\37\7s\2\2\37 \7t\2\2 !\7v\2\2!\4"+
		"\3\2\2\2\"#\7n\2\2#$\7q\2\2$%\7i\2\2%\6\3\2\2\2&(\t\2\2\2\'&\3\2\2\2("+
		")\3\2\2\2)\'\3\2\2\2)*\3\2\2\2*\b\3\2\2\2+-\t\2\2\2,+\3\2\2\2-.\3\2\2"+
		"\2.,\3\2\2\2./\3\2\2\2/\60\3\2\2\2\60\62\7\60\2\2\61\63\t\2\2\2\62\61"+
		"\3\2\2\2\63\64\3\2\2\2\64\62\3\2\2\2\64\65\3\2\2\2\65\n\3\2\2\2\668\t"+
		"\3\2\2\67\66\3\2\2\289\3\2\2\29\67\3\2\2\29:\3\2\2\2:;\3\2\2\2;<\b\6\2"+
		"\2<\f\3\2\2\2=A\t\4\2\2>@\t\5\2\2?>\3\2\2\2@C\3\2\2\2A?\3\2\2\2AB\3\2"+
		"\2\2B\16\3\2\2\2CA\3\2\2\2DE\7-\2\2E\20\3\2\2\2FG\7/\2\2G\22\3\2\2\2H"+
		"I\7,\2\2I\24\3\2\2\2JK\7\61\2\2K\26\3\2\2\2LM\7*\2\2M\30\3\2\2\2NO\7+"+
		"\2\2O\32\3\2\2\2PQ\7.\2\2Q\34\3\2\2\2\b\2).\649A\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}