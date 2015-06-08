/**
 * Copyright 2015 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-tidoop (FI-WARE project).
 *
 * fiware-tidoop is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-tidoop is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-tidoop. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with
 * francisco.romerobueno at telefonica dot com
 */
package com.telefonica.iot.tidoop.mrlib;

import bsh.EvalError;
import bsh.Interpreter;
import com.telefonica.iot.tidoop.mrlib.io.TidoopObject;
import com.telefonica.iot.tidoop.mrlib.io.NumericType;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *
 * @author frb
 */
public class Function {
    
    private final Logger logger = Logger.getLogger(Function.class);
    private Interpreter interpreter = null;
    private final NumericType type;
    private final String function;
    
    /**
     * Constructor.
     * @param functionStr
     */
    public Function(String functionStr) {
        String[] split = functionStr.split(" ");
        type = getType(split[0]);
        function = split[1];
        interpreter = new Interpreter(); // beanshell interpreter for the function
    } // Function
    
    /**
     * Evals y(x).
     * @param x
     * @return y
     */
    public TidoopObject eval(Text x) {
        try {
            switch (type) {
                case INT:
                    interpreter.set("x", new Integer(x.toString()));
                    break;
                case LONG:
                    interpreter.set("x", new Long(x.toString()));
                    break;
                case FLOAT:
                    interpreter.set("x", new Float(x.toString()));
                    break;
                case DOUBLE:
                    interpreter.set("x", new Double(x.toString()));
                    break;
                case BOOLEAN:
                    interpreter.set("x", Boolean.valueOf(x.toString()));
                    break;
                default:
                    interpreter.set("x", (Object) x);
            } // switch
            
            interpreter.eval(function);
            Object y = interpreter.get("y");
            // Object is not HDFS writabe, thus convert it to TidoopObject
            return TidoopObject.get(y, type);
        } catch (EvalError e) {
            logger.error("Error while evaluating the function. Details: " + e.getMessage());
            return null;
        } // try catch
    } // eval
    
    private NumericType getType(String typeStr) {
        if (typeStr.equals("int")) {
            return NumericType.INT;
        } else if (typeStr.equals("long")) {
            return NumericType.LONG;
        } else if (typeStr.equals("float")) {
            return NumericType.FLOAT;
        } else if (typeStr.equals("double")) {
            return NumericType.DOUBLE;
        } else if (typeStr.equals("boolean")) {
            return NumericType.BOOLEAN;
        } else {
            return NumericType.UNKNOWN;
        } // if else if
    } // getType
    
} // Function
