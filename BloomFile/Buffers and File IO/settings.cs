/*
    Copyright 2014 - Justin Weiler (justin@justinweiler.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
using System;
using System.IO;
using System.Xml;

namespace Phrenologix
{
    internal class settings
    {
        internal float blossomKeyCapacity;
        internal float errorRate;
        internal float padFactor;
        internal int averageDataItemSize;
        internal float averageDataItemSizeSlack;
        internal bool doComputeChecksum;
        internal byte[] branchFactors;
        internal long initialFileSize;
        internal long growFileSize;

        internal void saveSettings(string filePath)
        {
            XmlDocument xmlDoc = new XmlDocument();
            var settings = xmlDoc.CreateElement("Settings");
            xmlDoc.AppendChild(settings);

            settings.AppendChild(xmlDoc.CreateElement("BlossomKeyCapacity")).InnerXml = blossomKeyCapacity.ToString();
            settings.AppendChild(xmlDoc.CreateElement("ErrorRate")).InnerXml = errorRate.ToString();
            settings.AppendChild(xmlDoc.CreateElement("PadFactor")).InnerXml = padFactor.ToString();
            settings.AppendChild(xmlDoc.CreateElement("AvgDataItemSize")).InnerXml = averageDataItemSize.ToString();
            settings.AppendChild(xmlDoc.CreateElement("AvgDataItemSizeSlack")).InnerXml = averageDataItemSizeSlack.ToString();
            settings.AppendChild(xmlDoc.CreateElement("ComputeChecksum")).InnerXml = doComputeChecksum.ToString();
            settings.AppendChild(xmlDoc.CreateElement("BranchFactors")).InnerXml = Convert.ToBase64String(branchFactors);
            settings.AppendChild(xmlDoc.CreateElement("InitialFileSize")).InnerXml = initialFileSize.ToString();
            settings.AppendChild(xmlDoc.CreateElement("GrowFileSize")).InnerXml = growFileSize.ToString();

            xmlDoc.Save(filePath + ".xml");
        }

        internal void loadSettings(string filePath)
        {
            XmlDocument xmlDoc = new XmlDocument();

            xmlDoc.Load(filePath + ".xml");

            blossomKeyCapacity = float.Parse(xmlDoc.SelectSingleNode("Settings/BlossomKeyCapacity").InnerXml);
            errorRate = float.Parse(xmlDoc.SelectSingleNode("Settings/ErrorRate").InnerXml);
            padFactor = float.Parse(xmlDoc.SelectSingleNode("Settings/PadFactor").InnerXml);
            averageDataItemSize = int.Parse(xmlDoc.SelectSingleNode("Settings/AvgDataItemSize").InnerXml);
            averageDataItemSizeSlack = float.Parse(xmlDoc.SelectSingleNode("Settings/AvgDataItemSizeSlack").InnerXml);
            doComputeChecksum = bool.Parse(xmlDoc.SelectSingleNode("Settings/ComputeChecksum").InnerXml);
            branchFactors = Convert.FromBase64String(xmlDoc.SelectSingleNode("Settings/BranchFactors").InnerXml);
            initialFileSize = long.Parse(xmlDoc.SelectSingleNode("Settings/InitialFileSize").InnerXml);
            growFileSize = long.Parse(xmlDoc.SelectSingleNode("Settings/GrowFileSize").InnerXml);
        }
    }
}
