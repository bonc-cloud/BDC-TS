package cirrotimes

const (
	Separator        = "."
	SingleQuotationMark = "\""
	Wildcard         = "*"
	Comma            = ","
	SgPrefix         = "root.sg_"
	Select           = "select "
	From             = " from "
	GroupBy          = " group by "
	Where            = " where "
	Cpu              = "cpu"
	Time             = " time "
	Gte              = " >= "
	Lt               = " < "
	And              = " and "
	FiveSW           = Separator + Wildcard + Separator + Wildcard + Separator + Wildcard + Separator + Wildcard + Separator + Wildcard
	AirConditionRoom = "air_condition_room"
	Disk             = "disk"
	System           = "system"
	Ram              = "mem"
	UsePercent       = " used_percent "
	KapaLoad         = " load5, load15, load1 "
	KapaSuf          = SingleQuotationMark + "kapacitor_1" + SingleQuotationMark + NineSW
	MaxUse           = " max_value(usage_user) "
	AvgTem           = " avg(temperature) "
	LastUseAsMeanUse = " last_value(used_percent) AS mean_used_percent "
	LBracket         = "(["
	RBracket1M       = "), 1m)"
	RBracket1H       = "), 1h)"
	NineSW           = Separator + Wildcard + Separator + Wildcard + Separator + Wildcard + Separator + Wildcard + Separator + Wildcard + Separator + Wildcard + Separator + Wildcard + Separator + Wildcard + Separator + Wildcard
)
