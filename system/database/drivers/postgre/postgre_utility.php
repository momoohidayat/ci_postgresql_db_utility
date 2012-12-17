<?php  if ( ! defined('BASEPATH')) exit('No direct script access allowed');
/**
 * CodeIgniter
 *
 * An open source application development framework for PHP 5.1.6 or newer
 *
 * @package		CodeIgniter
 * @author		ExpressionEngine Dev Team
 * @copyright	Copyright (c) 2008 - 2011, EllisLab, Inc.
 * @license		http://codeigniter.com/user_guide/license.html
 * @link		http://codeigniter.com
 * @since		Version 1.0
 * @filesource
 */

// ------------------------------------------------------------------------

/**
 * Postgre Utility Class
 *
 * @category	Database
 * @author		ExpressionEngine Dev Team
 * @link		http://codeigniter.com/user_guide/database/
 */
class CI_DB_postgre_utility extends CI_DB_utility {

	/**
	 * List databases
	 *
	 * @access	private
	 * @return	bool
	 */
	function _list_databases()
	{
		return "SELECT datname FROM pg_database";
	}

	// --------------------------------------------------------------------

	/**
	 * Optimize table query
	 *
	 * Is table optimization supported in Postgre?
	 *
	 * @access	private
	 * @param	string	the table name
	 * @return	object
	 */
	function _optimize_table($table)
	{
		return FALSE;
	}

	// --------------------------------------------------------------------

	/**
	 * Repair table query
	 *
	 * Are table repairs supported in Postgre?
	 *
	 * @access	private
	 * @param	string	the table name
	 * @return	object
	 */
	function _repair_table($table)
	{
		return FALSE;
	}

	// --------------------------------------------------------------------

	/**
	 * Postgre Export
	 *
	 * @access	private
	 * @param	array	Preferences
	 * @return	mixed
	 */
	function _backup($params = array())
	{
		// Currently unsupported
		//return $this->db->display_error('db_unsuported_feature');

		// custom postgres backup below
		if (count($params) == 0)
		{
			return FALSE;
		}

		// Extract the prefs for simplicity
		extract($params);

		// Build the output
		$output = '';

		// sequences
		$sequences = $this->_get_sequence();
		$add_sequences = '';
		$alter_sequence = '';
		if($sequences)
		{
			$output .= '-- DROP SEQUENCE'.$newline;
			$add_sequences .= $newline.'-- CREATE SEQUENCE'.$newline;
			
			foreach($sequences as $sequence)
			{
				$table_last_value = $this->_get_last_table_value($sequence['table_name'],$sequence['column_name']);
				$setval_attr = ($table_last_value>0)? 'true': 'false';
				$setval = ($table_last_value>0)? $table_last_value: 1;
				$output .= "DROP SEQUENCE IF EXISTS ".$sequence['sequence_name']." CASCADE;".$newline;
				$add_sequences .= "CREATE SEQUENCE ".$sequence['sequence_name'].$newline;
				$add_sequences .= "	START WITH ".$sequence['start_value'].$newline;
				$add_sequences .= "	INCREMENT BY ".$sequence['increment'].$newline;
				$add_sequences .= "	MINVALUE ".$sequence['minimum_value'].$newline;
				$add_sequences .= "	MAXVALUE ".$sequence['maximum_value'].";".$newline;
				$alter_sequence .= $newline.$newline.'-- ALTER SEQUENCE '.$sequence['sequence_name'];
				$alter_sequence .= $newline.'ALTER SEQUENCE '.$sequence['sequence_name'].' OWNED BY '.$sequence['table_name'].'.'.$sequence['column_name'].';';
				$alter_sequence .= $newline."SELECT pg_catalog.setval('".$sequence['sequence_name']."', ".$setval.", ".$setval_attr.");";
				$alter_sequence .= $newline."ALTER TABLE ONLY ".$sequence['table_name']." ALTER COLUMN ".$sequence['column_name']." SET DEFAULT nextval('".$sequence['sequence_name']."'::regclass);";
			}
		}
		// foreign keys
		$foreign_keys = $this->_get_foreign_key();
		$add_foreign_key = '';
		if($foreign_keys)
		{
			$output .= $newline.'-- DROP FOREIGN KEY'.$newline;
			$add_foreign_key .= $newline.$newline.'-- FOREIGN KEY'.$newline;
			foreach($foreign_keys as $foreign_key)
			{
				$output .= "ALTER TABLE ".$foreign_key['table_name']." DROP CONSTRAINT ".$foreign_key['constraint_name']." CASCADE;".$newline;
				$add_foreign_key .= "ALTER TABLE ".$foreign_key['table_name']." ADD CONSTRAINT ".$foreign_key['constraint_name']." FOREIGN KEY (".$foreign_key['column_name'].") REFERENCES ".$foreign_key['references_table']." (".$foreign_key['references_field'].");".$newline;
			}
		}

		// Drop + create type
		$list_type = '';
		$types = $this->_get_type();
		if($types)
		{
			$output .= $newline.'-- DROP TYPE';
			foreach($types as $enumtype)
			{
	            $list_type .= $enumtype['enumtype'].',';
	        }
	        $list_type = rtrim($list_type,",");
			$output .= $newline.'DROP TYPE IF EXISTS '.$list_type.' CASCADE;'.$newline;

			$output .= $newline.'-- CREATE TYPE';
			foreach($types as $type)
			{
				$enumlabels = $this->_get_enumlabel($type['enumtype']);
				$list_enumlabel = '';
				foreach($enumlabels as $enumlabel)
				{
					$list_enumlabel .= "'".$enumlabel['enumlabel']."',";
				}
				$list_enumlabel = rtrim($list_enumlabel,",");
	            // Create type
				$output .= $newline."CREATE TYPE ".$type['enumtype']." AS ENUM(".$list_enumlabel.");";
	        }
		}

		$insert_str = '';
		foreach ((array)$tables as $table)
		{
			// Is the table in the "ignore" list?
			if (in_array($table, (array)$ignore, TRUE))
			{
				continue;
			}
			// Get the table schema
			$query = $this->db->field_data($table);

			// No result means the table name was invalid
			if ($query === FALSE)
			{
				continue;
			}

			// Write out the table schema
			$output .= $newline.$newline.'-- TABLE STRUCTURE FOR: '.$table;
			if ($add_drop == TRUE)
			{
				$output .= $newline.'DROP TABLE IF EXISTS '.$table.';';
			}

			$output .= $newline.'CREATE TABLE '.$table.' (';
			$i = 0;
			$count_field = count($query);
			$field_str = '';
			$is_int = array();
			$field_list = '';
			foreach ($query as $result)
			{
				// check if is integer
				$is_int[$i] = (in_array(
										strtolower($result->type),
										array('int2', 'int4', 'int8'), //, 'timestamp'),
										TRUE)
										) ? TRUE : FALSE;

				// Create a string of field names
				$field_str .= $result->name.", ";

				switch ($result->type)
				{
					case 'int2':
						$result->type = 'SMALLINT';
						break;
					case 'int4':
						$result->type = 'INTEGER';
						break;
					case 'bool':
						$result->type = 'BOOLEAN';
						break;
					case 'numeric':
						$length = $this->_check_length($table, $result->name, $result->type);
						$result->type = "NUMERIC(".$length['numeric_precision'].",".$length['numeric_scale'].")";
						break;
					case 'varchar':
						$length = $this->_check_length($table, $result->name, $result->type);
						$result->type = "VARCHAR(".$length['character_maximum_length'].")";
						break;
					case 'bpchar':
						$length = $this->_check_length($table, $result->name, $result->type);
						$result->type = "CHAR(".$length['character_maximum_length'].")";
						break;
					case 'text':
						$result->type = 'TEXT';
						break;
					case 'timestamp':
						$result->type = 'TIMESTAMP';
						break;
				}
				$is_primary = $this->_check_constraint($table, $result->name, 'PRIMARY KEY');
				$primary = ($is_primary) ? ' PRIMARY KEY': '';
				$is_unique = $this->_check_constraint($table, $result->name, 'UNIQUE');
				$unique = ($is_unique) ? ' UNIQUE': '';
				$output .= $newline.'	'.$result->name.' '.$result->type.$primary.$unique;
				
				if($i!=$count_field-1){ $output .= ',';}

				$i++;
			}
			$output .=$newline.');';
			
			// create insert statement

			// Trim off the end comma
			$field_str = rtrim($field_str, ", ");

			// Grab all the data from the current table
			$row_data = $this->db->query("SELECT * FROM $table");

			if ($row_data->num_rows() == 0)
			{
				continue;
			}

			// Build the insert string
			$insert_str .= $newline.$newline;
			$insert_str .= '-- INSERT DATA '.$table;
			$insert_num[$table] = $row_data->num_rows();
			foreach ($row_data->result_array() as $row)
			{
				$val_str = '';

				$i = 0;
				foreach ($row as $v)
				{
					// Is the value NULL?
					if ($v === NULL)
					{
						$val_str .= 'NULL';
					}
					else
					{
						// Escape the data if it's not an integer
						if ($is_int[$i] == FALSE)
						{
							$val_str .= $this->db->escape($v);
						}
						else
						{
							$val_str .= $v;
						}
					}

					// Append a comma
					$val_str .= ', ';
					$i++;
				}

				// Remove the comma at the end of the string
				$val_str = preg_replace( "/, $/" , "" , $val_str);

				// Build the INSERT string
				$insert_str .= $newline.'INSERT INTO '.$table.' ('.$field_str.') VALUES ('.$val_str.');';
			}	
		}

		$output .=  $add_sequences;
		$output .=  $insert_str;
		$output .=  $add_foreign_key;
		$output .=  $alter_sequence;
		// returning output
		return $output;
	}

	function _check_constraint($table=FALSE,$field=FALSE,$constraint=FALSE)
	{
		$this->db->select("
			tc.constraint_name, tc.table_name, kcu.column_name, 
		    ccu.table_name AS foreign_table_name,
		    ccu.column_name AS foreign_column_name 
		");
		$this->db->from("information_schema.table_constraints AS tc");
		$this->db->join("information_schema.key_column_usage AS kcu", "tc.constraint_name = kcu.constraint_name");
		$this->db->join("information_schema.constraint_column_usage AS ccu", "ccu.constraint_name = tc.constraint_name");
		$this->db->where("constraint_type", $constraint);
		$this->db->where("tc.table_name", $table);
		$this->db->where("kcu.column_name", $field);
		$result = $this->db->get()->row();
		return (count($result) > 0) ? TRUE : FALSE;
	}

	function _check_length($table,$field,$type)
	{
		if($type == 'bpchar' or $type == 'varchar') {
				$select_data = 'character_maximum_length';
		}
		elseif($type == 'numeric'){
			$select_data = 'numeric_precision,numeric_scale';
		}
		
		$result = $this->db->query("SELECT $select_data from INFORMATION_SCHEMA.COLUMNS where table_name='".$table."' AND column_name='".$field."' ")->row_array();
		return $result;
	}

	function _get_type()
	{
		$this->db->select("pg_type.typname AS enumtype");
		$this->db->from("pg_type");
		$this->db->join("pg_enum", "pg_enum.enumtypid = pg_type.oid");
		$this->db->group_by("pg_type.typname");
		$result = $this->db->get()->result_array();
		return (count($result) > 0) ? $result : FALSE;
	}

	function _get_enumlabel($enumtype)
	{
		$this->db->select("pg_enum.enumlabel AS enumlabel");
		$this->db->from("pg_type");
		$this->db->join("pg_enum", "pg_enum.enumtypid = pg_type.oid");
		$this->db->where("pg_type.typname", $enumtype);
		$result = $this->db->get()->result_array();
		return (count($result) > 0) ? $result : FALSE;
	}

	function _get_sequence()
	{
		$this->db->select("
			s.relname as sequence_name, 
			n.nspname as schema, 
			t.relname as table_name, 
			a.attname as column_name,
			sc.increment as increment, 
			sc.start_value as start_value, 
			sc.minimum_value as minimum_value, 
			sc.maximum_value as maximum_value");
		$this->db->from("pg_class s");
		$this->db->join("pg_depend d","d.objid=s.oid and d.classid='pg_class'::regclass and d.refclassid='pg_class'::regclass");
		$this->db->join("pg_class t","t.oid=d.refobjid");
		$this->db->join("pg_namespace n","n.oid=t.relnamespace");
		$this->db->join("pg_attribute a","a.attrelid=t.oid and a.attnum=d.refobjsubid");
		$this->db->join("information_schema.sequences sc","sc.sequence_name=s.relname");
		$this->db->where("s.relkind","S");
		$this->db->where("d.deptype","a");
		$this->db->where("sequence_schema","public");
		$result = $this->db->get()->result_array();
		return (count($result) > 0) ? $result : FALSE;
	}

	function _get_last_table_value($table,$field)
	{
		$return_data = 0;
		$this->db->select($field);
		$this->db->order_by($field,'DESC');
		$this->db->limit(1);
		$result = $this->db->get($table)->row_array();
		return (count($result)>0) ? $return_data = $result[$field] : $return_data;
	}

	function _get_foreign_key()
	{
		$result= $this->db->query("
            SELECT tc.constraint_name,
			tc.constraint_type,
			tc.table_name,
			kcu.column_name,
			rc.match_option AS match_type,

			rc.update_rule AS on_update,
			rc.delete_rule AS on_delete,
			ccu.table_name AS references_table,
			ccu.column_name AS references_field
			FROM information_schema.table_constraints tc

			LEFT JOIN information_schema.key_column_usage kcu
			ON tc.constraint_catalog = kcu.constraint_catalog
			AND tc.constraint_schema = kcu.constraint_schema
			AND tc.constraint_name = kcu.constraint_name

			LEFT JOIN information_schema.referential_constraints rc
			ON tc.constraint_catalog = rc.constraint_catalog
			AND tc.constraint_schema = rc.constraint_schema
			AND tc.constraint_name = rc.constraint_name

			LEFT JOIN information_schema.constraint_column_usage ccu
			ON rc.unique_constraint_catalog = ccu.constraint_catalog
			AND rc.unique_constraint_schema = ccu.constraint_schema
			AND rc.unique_constraint_name = ccu.constraint_name

			WHERE lower(tc.constraint_type) = 'foreign key' 

			ORDER BY tc.table_name")
		->result_array();
		return (count($result) > 0) ? $result : FALSE;
	}
}


/* End of file postgre_utility.php */
/* Location: ./system/database/drivers/postgre/postgre_utility.php */