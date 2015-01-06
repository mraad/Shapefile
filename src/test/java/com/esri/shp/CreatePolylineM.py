import arcpy


class CreatePolylineM(object):
    def __init__(self):
        self.label = "Create Tool"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramFC = arcpy.Parameter(
            name="links",
            displayName="links",
            direction="Output",
            datatype="DEFeatureClass",
            parameterType="Derived")
        return [paramFC]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        spref = arcpy.SpatialReference(102100)
        fc = "in_memory/Links"
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)
        arcpy.management.CreateFeatureclass("in_memory", "Links", "POLYLINE",
                                            spatial_reference=spref,
                                            has_m="ENABLED",
                                            has_z="DISABLED")

        cursor = arcpy.da.InsertCursor(fc, ['SHAPE@'])

        a = arcpy.Array()
        a.add(arcpy.Point(0, 0, 0, 10))
        a.add(arcpy.Point(100, 200, 0, 20))
        polylineM = arcpy.Polyline(a)
        cursor.insertRow([polylineM])

        parameters[0].value = fc
        return
