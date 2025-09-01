#!/usr/bin/env python3
"""
Unit tests for SaLS utility functions.

This module tests the core utility functions in util/util.py, including
configuration validation, file operations, and data processing functions.
"""

import pytest
import pandas as pd
import tempfile
import os
import sys
from datetime import datetime
from unittest.mock import patch, MagicMock

# Add the project root to the path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util import util


class TestConfigurationValidation:
    """Test configuration validation functions."""
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_validate_configuration_valid_config(self):
        """Test validation with a completely valid configuration."""
        config = {
            'queries': [{'test': 'test'}],
            'databases': ['arxiv', 'semantic_scholar'],
            'search_date': '2024-12-15',
            'folder_name': 'test_search'
        }
        
        is_valid, message, suggestions = util._validate_configuration(config, 'test.yaml')
        
        assert is_valid is True
        assert "Configuration validation passed successfully!" in message
        assert len(suggestions) == 0
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_validate_configuration_missing_queries(self):
        """Test validation with missing queries (critical error)."""
        config = {
            'databases': ['arxiv'],
            'search_date': '2024-12-15'
        }
        
        is_valid, message, suggestions = util._validate_configuration(config, 'test.yaml')
        
        assert is_valid is False
        assert "CRITICAL ERRORS" in message
        assert "Missing queries section" in message
        assert len(suggestions) > 0
        assert any(s['severity'] == 'critical' for s in suggestions)
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_validate_configuration_missing_optional_fields(self):
        """Test validation with missing optional fields (warnings only)."""
        config = {
            'queries': [{'test': 'test'}]
            # Missing databases, search_date, folder_name
        }
        
        is_valid, message, suggestions = util._validate_configuration(config, 'test.yaml')
        
        assert is_valid is True  # Can continue with warnings
        assert "WARNINGS" in message
        assert len(suggestions) > 0
        assert all(s['severity'] == 'warning' for s in suggestions)
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_validate_configuration_invalid_database(self):
        """Test validation with invalid database names."""
        config = {
            'queries': [{'test': 'test'}],
            'databases': ['invalid_db', 'arxiv']
        }
        
        is_valid, message, suggestions = util._validate_configuration(config, 'test.yaml')
        
        assert is_valid is True  # Can continue with warnings
        assert "invalid database" in message.lower()
        assert len(suggestions) > 0
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_validate_configuration_invalid_date_format(self):
        """Test validation with invalid date formats."""
        config = {
            'queries': [{'test': 'test'}],
            'start_date': 'invalid-date'
        }
        
        is_valid, message, suggestions = util._validate_configuration(config, 'test.yaml')
        
        assert is_valid is True  # Can continue with warnings
        assert "Invalid 'start_date' format" in message
        assert len(suggestions) > 0


class TestConfigurationFallbacks:
    """Test configuration fallback application."""
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_apply_configuration_fallbacks_missing_databases(self):
        """Test applying fallbacks for missing databases."""
        config = {'queries': [{'test': 'test'}]}
        suggestions = [{
            'issue': 'Missing databases section',
            'severity': 'warning',
            'default': ['arxiv', 'semantic_scholar']
        }]
        
        updated_config = util._apply_configuration_fallbacks(config, 'test.yaml', suggestions)
        
        assert 'databases' in updated_config
        assert updated_config['databases'] == ['arxiv', 'semantic_scholar']
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_apply_configuration_fallbacks_missing_search_date(self):
        """Test applying fallbacks for missing search_date."""
        config = {'queries': [{'test': 'test'}]}
        suggestions = [{
            'issue': 'Missing search_date',
            'severity': 'warning',
            'default': 'current date'
        }]
        
        with patch('util.util.datetime') as mock_datetime:
            mock_datetime.today.return_value = datetime(2024, 12, 15)
            mock_datetime.strftime.return_value = '2024-12-15'
            
            updated_config = util._apply_configuration_fallbacks(config, 'test.yaml', suggestions)
            
            assert 'search_date' in updated_config
            assert updated_config['search_date'] == '2024-12-15'
    
    @pytest.mark.unit
    @pytest.mark.config
    def test_apply_configuration_fallbacks_missing_folder_name(self):
        """Test applying fallbacks for missing folder_name."""
        config = {'queries': [{'test': 'test'}]}
        suggestions = [{
            'issue': 'Missing folder_name',
            'severity': 'warning',
            'default': 'filename-based'
        }]
        
        updated_config = util._apply_configuration_fallbacks(config, 'test.yaml', suggestions)
        
        assert 'folder_name' in updated_config
        assert updated_config['folder_name'] == 'test'


class TestQueryProcessing:
    """Test query processing and normalization functions."""
    
    @pytest.mark.unit
    def test_normalize_query_expression_basic(self):
        """Test basic query expression normalization."""
        expression = "'machine learning' & 'edge computing'"
        normalized = util.normalize_query_expression(expression)
        
        assert '<AND>' in normalized
        assert "'machine learning'" in normalized
        assert "'edge computing'" in normalized
    
    @pytest.mark.unit
    def test_normalize_query_expression_text_operators(self):
        """Test normalization of text-based operators."""
        expression = "'ml' AND 'edge' OR 'fog'"
        normalized = util.normalize_query_expression(expression)
        
        assert '<AND>' in normalized
        assert '<OR>' in normalized
        assert "'ml'" in normalized
        assert "'edge'" in normalized
        assert "'fog'" in normalized
    
    @pytest.mark.unit
    def test_normalize_query_expression_symbolic_operators(self):
        """Test normalization of symbolic operators."""
        expression = "'ml' && 'edge' || 'fog'"
        normalized = util.normalize_query_expression(expression)
        
        assert '<AND>' in normalized
        assert '<OR>' in normalized
        assert "'ml'" in normalized
        assert "'edge'" in normalized
        assert "'fog'" in normalized
    
    @pytest.mark.unit
    def test_normalize_query_expression_preserves_quotes(self):
        """Test that operators inside quotes are preserved."""
        expression = "'a & b' AND 'c | d'"
        normalized = util.normalize_query_expression(expression)
        
        assert "'a & b'" in normalized  # Preserved
        assert "'c | d'" in normalized  # Preserved
        assert '<AND>' in normalized    # Normalized
    
    @pytest.mark.unit
    def test_normalize_query_expression_encoding_artifacts(self):
        """Test removal of encoding artifacts."""
        expression = "Â'machine learning' & 'edge computing'"
        normalized = util.normalize_query_expression(expression)
        
        assert 'Â' not in normalized
        assert "'machine learning'" in normalized
        assert '<AND>' in normalized


class TestExponentialBackoff:
    """Test exponential backoff function."""
    
    @pytest.mark.unit
    def test_exponential_backoff_basic(self):
        """Test basic exponential backoff calculation."""
        delays = []
        for attempt in range(5):
            delay = util.exponential_backoff(attempt)
            delays.append(delay)
        
        # Should be increasing (with jitter)
        assert len(delays) == 5
        assert all(d > 0 for d in delays)
    
    @pytest.mark.unit
    def test_exponential_backoff_max_delay(self):
        """Test that max delay is respected."""
        delay = util.exponential_backoff(10, base_delay=1, max_delay=5)
        assert delay <= 5
    
    @pytest.mark.unit
    def test_exponential_backoff_custom_base(self):
        """Test custom base delay."""
        delay = util.exponential_backoff(2, base_delay=0.5)
        assert delay > 0.5  # Should be greater than base due to exponential growth


class TestDataProcessing:
    """Test data processing functions."""
    
    @pytest.mark.unit
    def test_remove_repeated_function_exists(self):
        """Test that the main remove_repeated function exists."""
        assert hasattr(util, 'remove_repeated')
        assert callable(util.remove_repeated)


class TestFileOperations:
    """Test file operation functions."""
    
    @pytest.mark.unit
    def test_save_function_creates_directory(self):
        """Test that save function creates directories if they don't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, 'subdir', 'test.csv')
            df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
            
            util.save(file_path, df, 'utf-8', 'w')
            
            assert os.path.exists(file_path)
            assert os.path.exists(os.path.dirname(file_path))
    
    @pytest.mark.unit
    def test_save_function_overwrites_existing(self):
        """Test that save function can overwrite existing files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.csv')
            df1 = pd.DataFrame({'col1': [1, 2, 3]})
            df2 = pd.DataFrame({'col1': [4, 5, 6]})
            
            # Save first DataFrame
            util.save(file_path, df1, 'utf-8', 'w')
            assert os.path.exists(file_path)
            
            # Overwrite with second DataFrame
            util.save(file_path, df2, 'utf-8', 'w')
            
            # Verify content was overwritten
            loaded_df = pd.read_csv(file_path)
            assert len(loaded_df) == 3
            assert loaded_df['col1'].iloc[0] == 4


class TestErrorHandling:
    """Test error handling and recovery."""
    
    @pytest.mark.unit
    @pytest.mark.error_handling
    def test_validate_configuration_exception_handling(self):
        """Test that validation handles unexpected exceptions gracefully."""
        with patch('util.util.datetime') as mock_datetime:
            mock_datetime.strptime.side_effect = Exception("Unexpected error")
            
            config = {
                'queries': [{'test': 'test'}],
                'start_date': '2024-01-01'
            }
            
            is_valid, message, suggestions = util._validate_configuration(config, 'test.yaml')
            
            assert is_valid is True  # Should continue with warnings
            assert "warning" in message.lower()
    
    @pytest.mark.unit
    @pytest.mark.error_handling
    def test_fallback_application_exception_handling(self):
        """Test that fallback application handles exceptions gracefully."""
        config = {'queries': [{'test': 'test'}]}
        suggestions = [{
            'issue': 'Test issue',
            'severity': 'warning',
            'default': 'test_default'
        }]
        
        # Mock a function that raises an exception
        with patch('util.util.logger.info', side_effect=Exception("Test exception")):
            result = util._apply_configuration_fallbacks(config, 'test.yaml', suggestions)
            
            # Should return original config even if fallback fails
            assert result == config


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"])
